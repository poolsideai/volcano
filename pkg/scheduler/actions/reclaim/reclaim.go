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
	"fmt"

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
		klog.V(3).Infof("Reclaiming resources for job <%s/%s>", pendingJob.Queue, pendingJob.Name)
		// it uses the PreemptiveFn of the capacity plugin to check if the queue can reclaim.
		// A queue can not reclaim when allocated + job.TotalRequest > deserved.
		if !ssn.Preemptive(ssn.Queues[pendingJob.Queue], pendingJob) {
			klog.V(3).Infof("[poolside] Job <%s/%s> can not reclaim resources due to overusage", pendingJob.Queue, pendingJob.Name)
			continue
		}

		if !jobPolicyAllowPeemption(pendingJob) {
			klog.V(3).Infof("Job <%s/%s> can not reclaim resources due to preemption policy", pendingJob.Queue, pendingJob.Name)
			continue
		}

		reclaimedEnough, reclaimedGPU, pendingJobTopology := getReclaimedResources(ssn, pendingJob.Clone(), runningJobs.Clone())
		if !reclaimedEnough {
			klog.V(3).Infof(`Job <%s/%s> can not reclaim resources due to not enough resources. Reclaimed GPU: <%d>, requested GPUs: <%d>`,
				pendingJob.Queue, pendingJob.Name, reclaimedGPU, pendingJob.GetTotalRequestGPU())

			continue
		}

		for pendingTaskName, task := range pendingJobTopology {
			for _, t := range task.TasksToEvict {
				err := ssn.Evict(t, fmt.Sprintf("reclaim for task <%s/%s>", string(pendingJob.Queue), pendingTaskName))
				if err != nil {
					klog.Errorf("Failed to evict task <%s/%s> for task <%s/%s>: %v",
						t.Namespace, t.Name, pendingJob.Namespace, pendingTaskName, err)
				}
			}
			// we still try to pipeline the task even if it fails to evict
			// because it might be a victim of a gang job
			if err := ssn.Pipeline(task.PendingTask, task.NodeName); err != nil {
				klog.Errorf("Failed to pipeline task <%s/%s>: %v", pendingJob.Namespace, pendingTaskName, err)
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

func getReclaimedResources(ssn *framework.Session, pendingJob *api.JobInfo, runningJobs *util.PriorityQueue) (bool, int64, map[string]*EvictTask) {
	reclaimedGPU := int64(0)
	reclaimedEnough := false
	finalPendingJobTopology := map[string]*EvictTask{}
	consideredJobs := []string{}
	for {
		if reclaimedEnough || runningJobs.Empty() {
			break
		}
		jobToEvict := runningJobs.Pop().(*api.JobInfo)
		if jobToEvict.Queue == pendingJob.Queue {
			continue
		}
		// first we check if the queue is overused
		if !isQueueOverused(ssn, jobToEvict) {
			klog.V(3).Infof("Job <%s/%s> can not be evicted because the queue is not overused", jobToEvict.Queue, jobToEvict.Name)
			continue
		}
		klog.V(3).Infof("JobToEvict: <%s/%s>", jobToEvict.Queue, jobToEvict.Name)
		consideredJobs = append(consideredJobs, fmt.Sprintf("%s/%s", jobToEvict.Queue, jobToEvict.Name))
		// then we need to check if the node can accommodate the task
		pendingJobTopology := findNodesForPendingJob(ssn, jobToEvict, pendingJob)
		if len(pendingJobTopology) == 0 {
			klog.V(3).Infof("Job <%s/%s> can not be evicted because the node can not accommodate the task", jobToEvict.Queue, jobToEvict.Name)
			continue
		}

		for _, n := range pendingJobTopology {
			finalPendingJobTopology[n.PendingTask.Name] = n
			// Count total reclaimed capacity (idle + evicted) allocated to this pending task
			reclaimedGPU += n.GPU
		}

		if reclaimedGPU >= pendingJob.GetTotalRequestGPU() {
			reclaimedEnough = true
			break
		}
	}
	if reclaimedEnough {
		klog.V(3).Infof("[poolside] Job <%s/%s> will reclaim enough resources: %v", pendingJob.Queue, pendingJob.Name, finalPendingJobTopology)
	} else {
		klog.V(3).Infof("[poolside] Job <%s/%s> cannot reclaim resources due to not enough resources. Considered jobs: %v", pendingJob.Queue, pendingJob.Name, consideredJobs)
	}
	return reclaimedEnough, reclaimedGPU, finalPendingJobTopology
}

func isQueueOverused(ssn *framework.Session, victimJob *api.JobInfo) bool {
	queueAllocatedGPUs := ssn.Queues[victimJob.Queue].GetAllocatedGPU()
	queueDeservedGPUs := ssn.Queues[victimJob.Queue].GetDeservedGPU()
	return queueAllocatedGPUs > queueDeservedGPUs
}

type EvictTask struct {
	NodeName     string
	GPU          int64
	TasksToEvict []*api.TaskInfo
	PendingTask  *api.TaskInfo
}

func (e *EvictTask) String() string {
	tasks := []string{}
	for _, t := range e.TasksToEvict {
		tasks = append(tasks, t.Name)
	}
	return fmt.Sprintf("[PendingTask: %s, NodeName: %s, TasksToEvict: %v]", e.PendingTask.Name, e.NodeName, tasks)
}

func findNodesForPendingJob(ssn *framework.Session, victimJob, pendingJob *api.JobInfo) map[string]*EvictTask {
	// Build per-node capacity with idle counted once and evictable tasks listed
	type nodeCapacity struct {
		nodeName     string
		idleGPU      int64
		evictable    []*api.TaskInfo
		evictableGPU int64
	}

	nodeCaps := map[string]*nodeCapacity{}
	for _, t := range victimJob.Tasks {
		node := ssn.Nodes[t.NodeName]
		if node == nil {
			continue
		}
		gpu := int64(t.Resreq.ScalarResources["nvidia.com/gpu"])
		cap, ok := nodeCaps[t.NodeName]
		if !ok {
			cap = &nodeCapacity{
				nodeName:     t.NodeName,
				idleGPU:      int64(node.FutureIdle().ScalarResources["nvidia.com/gpu"]),
				evictable:    []*api.TaskInfo{},
				evictableGPU: 0,
			}
			nodeCaps[t.NodeName] = cap
		}
		cap.evictable = append(cap.evictable, t)
		cap.evictableGPU += gpu
	}

	// record task name -> node name so we know how to pipeline the tasks later
	pendingJobTopology := map[string]*EvictTask{}
	for _, task := range pendingJob.Tasks {
		required := int64(task.Resreq.ScalarResources["nvidia.com/gpu"])
		placed := false
		for _, cap := range nodeCaps {
			totalAvail := cap.idleGPU + cap.evictableGPU
			if totalAvail < required {
				continue
			}

			// simulate consumption on this node
			remainingIdle := cap.idleGPU
			remainingTasks := make([]*api.TaskInfo, len(cap.evictable))
			copy(remainingTasks, cap.evictable)

			result := &EvictTask{
				NodeName:    cap.nodeName,
				PendingTask: task,
				GPU:         0,
			}

			// use idle first
			if remainingIdle > 0 {
				use := remainingIdle
				if use > required {
					use = required
				}
				remainingIdle -= use
				required -= use
				result.GPU += use
			}

			// then evict tasks until satisfied
			evictedIdx := 0
			for required > 0 && evictedIdx < len(remainingTasks) {
				vt := remainingTasks[evictedIdx]
				g := int64(vt.Resreq.ScalarResources["nvidia.com/gpu"])
				result.TasksToEvict = append(result.TasksToEvict, vt)
				result.GPU += g
				required -= g
				evictedIdx++
			}

			if required > 0 {
				// not enough even after evictions; try another node
				continue
			}

			// commit consumption to this node capacity
			cap.idleGPU = remainingIdle
			if evictedIdx >= len(remainingTasks) {
				cap.evictable = []*api.TaskInfo{}
			} else {
				cap.evictable = remainingTasks[evictedIdx:]
			}
			// recompute evictableGPU after removing evicted tasks
			newEvictableGPU := int64(0)
			for _, vt := range cap.evictable {
				newEvictableGPU += int64(vt.Resreq.ScalarResources["nvidia.com/gpu"])
			}
			cap.evictableGPU = newEvictableGPU

			pendingJobTopology[task.Name] = result
			delete(pendingJob.Tasks, task.UID)
			placed = true
			break
		}
		if !placed {
			// could not place this pending task with this victim job's nodes
			continue
		}
	}
	return pendingJobTopology
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
