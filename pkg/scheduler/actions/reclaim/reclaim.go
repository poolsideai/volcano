/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added job validation and preemption policy support
- Enhanced victim selection with priority queue ordering
- Added PrePredicate validation and node filtering

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reclaim

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (ra *Action) Name() string {
	return "reclaim"
}

func (ra *Action) Initialize() {}

func (ra *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Reclaim ...")
	defer klog.V(5).Infof("Leaving Reclaim ...")

	pendingJobs := util.NewPriorityQueue(preemptorJobOrder(ssn))
	runningJobs := util.NewPriorityQueue(preempteeJobOrder(ssn))

	for _, job := range ssn.Jobs {
		// job.IsPending check if the job has passed the Enqueue phase.
		// We shouldn't really have any pending job in terms of Volcano here.
		if job.IsPending() {
			continue
		}
		// When job is starving, it means the job is pending to scheduled in our terms.
		if job.IsStarving() {
			pendingJobs.Push(job)
		} else {
			runningJobs.Push(job)
		}
	}

	for {
		if pendingJobs.Empty() {
			break
		}

		pendingJob := pendingJobs.Pop().(*api.JobInfo)
		// it uses the PreemptiveFn of the capacity plugin to check if the queue can reclaim.
		// A queue can not reclaim when allocated + job.TotalRequest > deserved.
		if !ssn.Preemptive(ssn.Queues[pendingJob.Queue], pendingJob) {
			klog.V(3).Infof("Job <%s/%s> can not reclaim resources due to overusage", pendingJob.Queue, pendingJob.Name)
			continue
		}

		if !jobPolicyAllowPeemption(pendingJob) {
			klog.V(3).Infof("Job <%s/%s> can not reclaim resources due to preemption policy", pendingJob.Queue, pendingJob.Name)
			continue
		}

		reclaimedEnough, reclaimedGPU, pendingJobTopology, jobsToRequeue := getReclaimedResources(ssn, pendingJob.Clone(), runningJobs)
		// push back the jobs that would not be reclaimed
		for _, victim := range jobsToRequeue {
			runningJobs.Push(victim)
		}
		if !reclaimedEnough {
			klog.V(3).Infof(`Job <%s/%s> can not reclaim resources due to not enough resources. Reclaimed GPU: <%d>, requested GPUs: <%d>`,
				pendingJob.Queue, pendingJob.Name, reclaimedGPU, pendingJob.GetTotalRequestGPU())

			continue
		}

		for _, task := range pendingJobTopology {
			for _, t := range task.TasksToEvict {
				err := ssn.Evict(t, "reclaim for job "+pendingJob.Name)
				if err != nil {
					klog.Errorf("Failed to evict task <%s/%s> for job <%s/%s>: %v",
						t.Namespace, t.Name, pendingJob.Namespace, pendingJob.Name, err)
				}
			}
			// we still try to pipeline the task even if it fails to evict
			// because it might be a victim of a gang job
			if err := ssn.Pipeline(task.PendingTask, task.NodeName); err != nil {
				klog.Errorf("Failed to pipeline job <%s/%s>: %v", pendingJob.Namespace, pendingJob.Name, err)
			}
		}
	}
}

func (ra *Action) UnInitialize() {
}

func jobPolicyAllowPeemption(job *api.JobInfo) bool {
	tasks := job.TaskStatusIndex[api.Pending]
	for _, task := range tasks {
		if task.Pod.Spec.PreemptionPolicy != nil && *task.Pod.Spec.PreemptionPolicy == v1.PreemptNever {
			return false
		}
	}
	return true
}

func getReclaimedResources(ssn *framework.Session, pendingJob *api.JobInfo, runningJobs *util.PriorityQueue) (bool, int64, map[string]*EvictTask, []*api.JobInfo) {
	reclaimedGPU := int64(0)
	reclaimedEnough := false
	finalVictims := []*api.JobInfo{}
	skippedVictims := []*api.JobInfo{}
	finalPendingJobTopology := map[string]*EvictTask{}
	for {
		if reclaimedEnough || runningJobs.Empty() {
			break
		}
		jobToEvict := runningJobs.Pop().(*api.JobInfo)
		if jobToEvict.Queue == pendingJob.Queue {
			skippedVictims = append(skippedVictims, jobToEvict)
			continue
		}
		klog.V(3).Infof("Checking if job <%s/%s> can be evicted", jobToEvict.Queue, jobToEvict.Name)
		// first we check if the queue is overused
		if !isQueueOverused(ssn, jobToEvict) {
			klog.V(3).Infof("Job <%s/%s> can not be evicted because the queue is not overused", jobToEvict.Queue, jobToEvict.Name)
			skippedVictims = append(skippedVictims, jobToEvict)
			continue
		}
		// then we need to check if the node can accommodate the task
		pendingJobTopology := findNodesForPendingJob(ssn, jobToEvict, pendingJob)
		if len(pendingJobTopology) == 0 {
			skippedVictims = append(skippedVictims, jobToEvict)
			continue
		}

		for _, n := range pendingJobTopology {
			finalPendingJobTopology[n.PendingTask.Name] = n
			reclaimedGPU += n.GPU
		}

		finalVictims = append(finalVictims, jobToEvict)
		if reclaimedGPU >= pendingJob.GetTotalRequestGPU() {
			reclaimedEnough = true
			break
		}
	}
	// we need to requeue the skipped jobs because they might be victims of other jobs
	jobsToRequeue := skippedVictims
	if !reclaimedEnough {
		// we need to include the final victims because we didn't reclaim enough so they won't be evicted
		jobsToRequeue = append(jobsToRequeue, finalVictims...)
	}
	return reclaimedEnough, reclaimedGPU, finalPendingJobTopology, jobsToRequeue
}

func isQueueOverused(ssn *framework.Session, victimJob *api.JobInfo) bool {
	queueAllocatedGPUs := ssn.Queues[victimJob.Queue].GetAllocatedGPU()
	queueDeservedGPUs := ssn.Queues[victimJob.Queue].GetDeservedGPU()
	return queueAllocatedGPUs > queueDeservedGPUs
}

func noBudgetViolationAfterReclaim(ssn *framework.Session, victimJob, pendingJob *api.JobInfo) bool {
	queueAllocatedGPUs := ssn.Queues[victimJob.Queue].GetAllocatedGPU()
	queueDeservedGPUs := ssn.Queues[victimJob.Queue].GetDeservedGPU()

	withoutVictimJob := int64(0)
	if len(victimJob.Tasks) == int(victimJob.MinAvailable) {
		// when it's not an elastic workload, we check the total request of the job
		withoutVictimJob = queueAllocatedGPUs - victimJob.GetTotalRequestGPU()
	} else {
		// when it's an elastic workload, we check the minimum between the elastic GPUs and the requested GPUs of the victim job
		elasticGPUs := victimJob.GetElasticGPUs()
		requestedGPUs := pendingJob.GetTotalRequestGPU()
		withoutVictimJob = queueAllocatedGPUs - min(elasticGPUs, requestedGPUs)
	}
	if queueDeservedGPUs <= withoutVictimJob {
		return true
	}
	return false
}

type EvictTask struct {
	NodeName     string
	GPU          int64
	TasksToEvict []*api.TaskInfo
	PendingTask  *api.TaskInfo
}

func findNodesForPendingJob(ssn *framework.Session, victimJob, pendingJob *api.JobInfo) map[string]*EvictTask {
	// topology maps have "required number of GPUs" -> "node names"
	// {1: ["node1", "node2"]} means the nodes
	// using the VictimTask struct instead of a single node name string because we need to carry the task information
	// and the idle gpu count on that node for the calculation below
	victimNodes := map[string]*EvictTask{}
	for _, task := range victimJob.Tasks {
		node := ssn.Nodes[task.NodeName]
		if node == nil {
			continue
		}
		// we need to consider future idle in case the node wasn't fully occupied by the victim task
		nodeFutureIdleGPU := node.FutureIdle().ScalarResources["nvidia.com/gpu"]
		numGPU := int64(task.Resreq.ScalarResources["nvidia.com/gpu"] + nodeFutureIdleGPU)
		tasks, ok := victimNodes[task.NodeName]
		if !ok {
			victimNodes[task.NodeName] = &EvictTask{
				NodeName:     task.NodeName,
				GPU:          numGPU,
				TasksToEvict: []*api.TaskInfo{task},
			}
		} else {
			victimNodes[task.NodeName] = &EvictTask{
				NodeName:     task.NodeName,
				GPU:          tasks.GPU + numGPU,
				TasksToEvict: append(tasks.TasksToEvict, task),
			}
		}
	}
	// record task name -> node name so we know how to pipeline the tasks later
	pendingJobTopology := map[string]*EvictTask{}
	for _, task := range pendingJob.Tasks {
		requiredGPU := int64(task.Resreq.ScalarResources["nvidia.com/gpu"])
		for _, node := range victimNodes {
			if node.GPU < requiredGPU {
				continue
			}
			result := &EvictTask{
				NodeName:    node.NodeName,
				PendingTask: task,
				GPU:         int64(0),
			}
			if len(node.TasksToEvict) == 0 {
				node.GPU -= requiredGPU
				result.GPU = requiredGPU
				pendingJobTopology[task.Name] = result
				delete(pendingJob.Tasks, task.UID)
				break
			}
			for idx, t := range node.TasksToEvict {
				requiredGPU -= int64(t.Resreq.ScalarResources["nvidia.com/gpu"])
				result.TasksToEvict = append(result.TasksToEvict, t)
				result.GPU += int64(t.Resreq.ScalarResources["nvidia.com/gpu"])
				node.GPU -= int64(t.Resreq.ScalarResources["nvidia.com/gpu"])
				node.TasksToEvict = deleteFromSlice(node.TasksToEvict, idx)
				if requiredGPU == 0 {
					break
				}
			}
			pendingJobTopology[task.Name] = result
			delete(pendingJob.Tasks, task.UID)
			break
		}
	}
	return pendingJobTopology
}

func deleteFromSlice[T any](slice []T, index int) []T {
	if index < 0 || index >= len(slice) {
		return slice
	}
	if index == 0 {
		return slice[1:]
	}
	if index == len(slice)-1 {
		return slice[:index]
	}
	return append(slice[:index], slice[index+1:]...)
}

func preempteeJobOrder(ssn *framework.Session) func(l, r interface{}) bool {
	return func(l, r interface{}) bool {
		lJob := l.(*api.JobInfo)
		rJob := r.(*api.JobInfo)

		lvElasticResources := lJob.GetElasticGPUs()
		rvElasticResources := rJob.GetElasticGPUs()

		if lvElasticResources != rvElasticResources {
			// this will be used as a LessThan function in building up the heap,
			// so we need to return the opposite of the comparison to prioritize the job with more elastic replicas
			return lvElasticResources > rvElasticResources
		}

		// when jobs have the same elastic replicas, we compare the queue priorities
		lQueue := ssn.Queues[lJob.Queue]
		rQueue := ssn.Queues[rJob.Queue]

		if lQueue.Queue.Spec.Priority != rQueue.Queue.Spec.Priority {
			return lQueue.Queue.Spec.Priority < rQueue.Queue.Spec.Priority
		}

		// when jobs have the same elastic replicas and queue priorities, we compare the queue overusage
		lvOverusage := getQueueOverusage(lQueue)
		rvOverusage := getQueueOverusage(rQueue)
		if lvOverusage != rvOverusage {
			// we want to prioritize the queue with more overusage
			return lvOverusage > rvOverusage
		}

		// we compare the priorities of the jobs
		if lJob.Priority != rJob.Priority {
			return lJob.Priority < rJob.Priority
		}

		// compare the number of tasks in a job, and we prioritize the job with fewer tasks
		lTasks := len(lJob.Tasks)
		rTasks := len(rJob.Tasks)
		if lTasks != rTasks {
			return lTasks < rTasks
		}

		// lastly we compare the job creation timestamp
		return lJob.CreationTimestamp.Before(&rJob.CreationTimestamp)
	}
}

func getQueueOverusage(queue *api.QueueInfo) float64 {
	allocatedGPUs := queue.GetAllocatedGPU()
	deservedGPUs := queue.GetDeservedGPU()
	overusage := float64(allocatedGPUs-deservedGPUs) / float64(deservedGPUs)
	if overusage < 0 {
		return 0
	}
	return overusage
}

func preemptorJobOrder(ssn *framework.Session) func(l, r interface{}) bool {
	return func(l, r interface{}) bool {
		lJob := l.(*api.JobInfo)
		rJob := r.(*api.JobInfo)

		lQueue := ssn.Queues[lJob.Queue]
		rQueue := ssn.Queues[rJob.Queue]

		if lQueue.Queue.Spec.Priority != rQueue.Queue.Spec.Priority {
			return lQueue.Queue.Spec.Priority > rQueue.Queue.Spec.Priority
		}

		if lJob.Priority != rJob.Priority {
			return lJob.Priority > rJob.Priority
		}

		// we need to prioritize the job that gets created later
		// otherwise the new job will just keep evicting jobs but don't get the resource
		return rJob.CreationTimestamp.Before(&lJob.CreationTimestamp)
	}
}
