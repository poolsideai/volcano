*
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

	pendingJobs := util.NewPriorityQueue(ssn.PreemptorJobOrderFn)
	victimJobs := util.NewPriorityQueue(ssn.PreempteeJobOrderFn)

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
			victimJobs.Push(job)
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
			klog.V(3).Infof("Job <%s/%s> Queue <%s> can not reclaim resources due to overusage", pendingJob.Namespace, pendingJob.Name, pendingJob.Queue)
			continue
		}

		if !jobPolicyAllowPeemption(pendingJob) {
			klog.V(3).Infof("Job <%s/%s> Queue <%s> can not reclaim resources due to preemption policy", pendingJob.Namespace, pendingJob.Name, pendingJob.Queue)
			continue
		}

		reclaimedResource := api.EmptyResource()
		reclaimedEnough := false
		finalVictims := []*api.JobInfo{}
		for {
			if reclaimedEnough || victimJobs.Empty() {
				break
			}
			victimJob := victimJobs.Pop().(*api.JobInfo)
			finalVictims = append(finalVictims, victimJob)
			reclaimedResource.Add(victimJob.TotalRequest)
			if reclaimedResource.LessEqual(pendingJob.TotalRequest, api.Zero) {
				reclaimedEnough = true
				break
			}
		}

		if !reclaimedEnough {
			klog.V(3).Infof(`Job <%s/%s> Queue <%s> can not reclaim resources due to not enough resources. 
			Reclaimed resources: <%v>, requested resources: <%v>`,
				pendingJob.Namespace, pendingJob.Name, pendingJob.Queue, reclaimedResource, pendingJob.TotalRequest)

			// push back the victims that were not reclaimed
			for _, victim := range finalVictims {
				victimJobs.Push(victim)
			}
			continue
		}

		for _, victim := range finalVictims {
			errs := evictJob(ssn, victim, pendingJob.Name)
			if len(errs) > 0 {
				klog.Errorf("Failed to reclaim job <%s/%s> for job <%s/%s>: %v",
					victim.Namespace, victim.Name, pendingJob.Namespace, pendingJob.Name, errs)
				continue
			}
		}

		// since killing one task in a job terminates the whole job, we try to pipeline the job here
		if err := ssn.Pipeline(pendingJob, ""); err != nil {
			klog.Errorf("Failed to pipeline job <%s/%s>: %v", pendingJob.Namespace, pendingJob.Name, err)
			continue
		}
	}

	// queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	// queueMap := map[api.QueueID]*api.QueueInfo{}

	// preemptorsMap := map[api.QueueID]*util.PriorityQueue{}
	// preemptorTasks := map[api.JobID]*util.PriorityQueue{}
	// preempteesMap := map[api.QueueID]*util.PriorityQueue{}

	// klog.V(3).Infof("There are <%d> Jobs and <%d> Queues in total for scheduling.",
	// 	len(ssn.Jobs), len(ssn.Queues))

	// for _, job := range ssn.Jobs {
	// 	if job.IsPending() {
	// 		continue
	// 	}

	// 	if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
	// 		klog.V(4).Infof("Job <%s/%s> Queue <%s> skip reclaim, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
	// 		continue
	// 	}
	// 	queue, found := ssn.Queues[job.Queue]
	// 	if !found {
	// 		klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>", job.Queue, job.Namespace, job.Name)
	// 		continue
	// 	}
	// 	if _, found := queueMap[queue.UID]; !found {
	// 		klog.V(4).Infof("Added Queue <%s> for Job <%s/%s>", queue.Name, job.Namespace, job.Name)
	// 		queueMap[queue.UID] = queue
	// 		queues.Push(queue)
	// 	}

	// 	// if it's starving, it means the job is pending for more resources
	// 	if ssn.JobStarving(job) {
	// 		if _, found := preemptorsMap[job.Queue]; !found {
	// 			preemptorsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
	// 		}
	// 		preemptorsMap[job.Queue].Push(job)
	// 		preemptorTasks[job.UID] = util.NewPriorityQueue(ssn.TaskOrderFn)
	// 		for _, task := range job.TaskStatusIndex[api.Pending] {
	// 			if task.SchGated {
	// 				continue
	// 			}
	// 			preemptorTasks[job.UID].Push(task)
	// 		}
	// 	} else {
	// 		if _, found := preempteesMap[job.Queue]; !found {
	// 			preempteesMap[job.Queue] = util.NewPriorityQueue(VictimJobOrder)
	// 		}
	// 		preempteesMap[job.Queue].Push(job)
	// 	}
	// }

	// for {
	// 	// If no queues, break
	// 	if queues.Empty() {
	// 		break
	// 	}

	// 	var job *api.JobInfo
	// 	var task *api.TaskInfo

	// 	queue := queues.Pop().(*api.QueueInfo)
	// 	if ssn.Overused(queue) {
	// 		klog.V(3).Infof("Queue <%s> is overused <%v>, ignore it.", queue.Name, queue.Queue.Status.Allocated)
	// 		continue
	// 	}

	// 	// Get all jobs based on the queue and the queue is selected based on the QueueOrderFn on L50
	// 	jobs, found := preemptorsMap[queue.UID]
	// 	if !found || jobs.Empty() {
	// 		continue
	// 	}
	// 	// The job is selected based on the JobOrderFn on L80
	// 	job = jobs.Pop().(*api.JobInfo)

	// 	// TODO: job doesn't have the preemptionPolicy information, so we pop one task to check that for now
	// 	tasks, found := preemptorTasks[job.UID]
	// 	if !found || tasks.Empty() {
	// 		queues.Push(queue)
	// 		continue
	// 	}
	// 	task = tasks.Pop().(*api.TaskInfo)
	// 	if task.Pod.Spec.PreemptionPolicy != nil && *task.Pod.Spec.PreemptionPolicy == v1.PreemptNever {
	// 		klog.V(3).Infof("Task %s/%s is not eligible to preempt other tasks due to preemptionPolicy is Never", task.Namespace, task.Name)
	// 		queues.Push(queue)
	// 		continue
	// 	}

	// 	// Here we're mainly using the PreemptiveFn of the capacity plugin to check if the queue can reclaim
	// 	if !ssn.Preemptive(queue, job) {
	// 		klog.V(3).Infof("Queue <%s> can not reclaim by evicting others when considering job <%s> , ignore it.", queue.Name, job.Name)
	// 		continue
	// 	}

	// 	// TODO: we should use PrePredicateFn to check all tasks in the job
	// 	if err := ssn.PrePredicateFn(task); err != nil {
	// 		klog.V(3).Infof("PrePredicate for task %s/%s failed for: %v", task.Namespace, task.Name, err)
	// 		continue
	// 	}

	// 	assigned := false
	// 	// we should filter out those nodes that are UnschedulableAndUnresolvable status got in allocate action
	// 	totalNodes := ssn.FilterOutUnschedulableAndUnresolvableNodesForTask(task)
	// 	for _, n := range totalNodes {
	// 		// When filtering candidate nodes, need to consider the node statusSets instead of the err information.
	// 		// refer to kube-scheduler preemption code: https://github.com/kubernetes/kubernetes/blob/9d87fa215d9e8020abdc17132d1252536cd752d2/pkg/scheduler/framework/preemption/preemption.go#L422
	// 		if err := ssn.PredicateForPreemptAction(task, n); err != nil {
	// 			klog.V(4).Infof("Reclaim predicate for task %s/%s on node %s return error %v ", task.Namespace, task.Name, n.Name, err)
	// 			continue
	// 		}

	// 		klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.", task.Namespace, task.Name, n.Name)

	// 		var reclaimees []*api.TaskInfo
	// 		for _, task := range n.Tasks {
	// 			// Ignore non running task.
	// 			if task.Status != api.Running {
	// 				continue
	// 			}
	// 			if !task.Preemptable {
	// 				continue
	// 			}

	// 			if j, found := ssn.Jobs[task.Job]; !found {
	// 				continue
	// 			} else if j.Queue != job.Queue {
	// 				q := ssn.Queues[j.Queue]
	// 				if !q.Reclaimable() {
	// 					continue
	// 				}
	// 				// Clone task to avoid modify Task's status on node.
	// 				reclaimees = append(reclaimees, task.Clone())
	// 			}
	// 		}

	// 		if len(reclaimees) == 0 {
	// 			klog.V(3).Infof("No reclaimees on Node <%s>.", n.Name)
	// 			continue
	// 		}

	// 		victims := ssn.Reclaimable(task, reclaimees)

	// 		if err := util.ValidateVictims(task, n, victims); err != nil {
	// 			klog.V(3).Infof("No validated victims on Node <%s>: %v", n.Name, err)
	// 			continue
	// 		}

	// 		victimsQueue := ssn.BuildVictimsPriorityQueue(victims, task)

	// 		resreq := task.InitResreq.Clone()
	// 		reclaimed := n.FutureIdle()

	// 		// Reclaim victims for tasks.
	// 		for !victimsQueue.Empty() {
	// 			reclaimee := victimsQueue.Pop().(*api.TaskInfo)
	// 			klog.Errorf("Try to reclaim Task <%s/%s> for Tasks <%s/%s>",
	// 				reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name)
	// 			if err := ssn.Evict(reclaimee, "reclaim for task "+task.Name); err != nil {
	// 				klog.Errorf("Failed to reclaim Task <%s/%s> for Tasks <%s/%s>: %v",
	// 					reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name, err)
	// 				continue
	// 			}
	// 			reclaimed.Add(reclaimee.Resreq)
	// 			// If reclaimed enough resources, break loop to avoid Sub panic.
	// 			if resreq.LessEqual(reclaimed, api.Zero) {
	// 				break
	// 			}
	// 		}

	// 		klog.V(3).Infof("Reclaimed <%v> for task <%s/%s> requested <%v>.",
	// 			reclaimed, task.Namespace, task.Name, task.InitResreq)

	// 		if task.InitResreq.LessEqual(reclaimed, api.Zero) {
	// 			if err := ssn.Pipeline(task, n.Name); err != nil {
	// 				klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
	// 					task.Namespace, task.Name, n.Name)
	// 			}

	// 			// Ignore error of pipeline, will be corrected in next scheduling loop.
	// 			assigned = true

	// 			break
	// 		}
	// 	}

	// 	if assigned {
	// 		jobs.Push(job)
	// 	}
	// 	queues.Push(queue)
	// }
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

// TODO: check if we need to evict all tasks or not because
// we have TerminateJob
func evictJob(ssn *framework.Session, victimJob *api.JobInfo, pendingJobName string) []error {
	numberOfTasksToEvict := len(victimJob.Tasks) - int(victimJob.TaskMinAvailableTotal)
	if numberOfTasksToEvict <= 0 {
		numberOfTasksToEvict = len(victimJob.Tasks)
	}
	i := 0
	errors := []error{}
	// TODO: we should iterate tasks based on node to have better bin packing
	for _, task := range victimJob.Tasks {
		err := ssn.Evict(task, "reclaim for job "+pendingJobName)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to evict task <%s/%s>: %v", task.Namespace, task.Name, err))
		}
		i++
		if i >= numberOfTasksToEvict {
			break
		}
	}
	return errors
}
