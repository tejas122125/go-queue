package integration_test

import (
	"fmt"
	"math"
	"testing"
)

type JobStatus string

const (
	StatusPending     JobStatus = "PENDING"
	StatusScheduled   JobStatus = "SCHEDULED"
	StatusRunning     JobStatus = "RUNNING"
	StatusCompleted   JobStatus = "COMPLETED"
	StatusRetrying    JobStatus = "RETRYING"
	StatusDeadLetter  JobStatus = "DEAD_LETTER"
	StatusCanceled    JobStatus = "CANCELED"
)

type Job struct {
	ID          string
	Name        string
	Priority    int
	Status      JobStatus
	Attempts    int
	MaxAttempts int
	Version     int
	RunAt       *int64
}

type OutboxEvent struct {
	ID               string
	JobID            string
	Priority         int
	Processed        bool
	Attempts         int
	DeduplicationKey string
}

type Lease struct {
	JobID        string
	WorkerID     string
	FencingToken int
	ExpiresAt    int64
}

type delayedItem struct {
	JobID string
	DueAt int64
}

type CreateJobInput struct {
	Name        string
	Priority    int
	MaxAttempts *int
	RunAt       *int64
}

type PublishOptions struct {
	CrashAfterPublish bool
}

// InMemoryQueueForge mirrors the complete pipeline simulation from pipeline.integration.test.ts.
type InMemoryQueueForge struct {
	nextID      int
	Now         int64
	Paused      bool
	Jobs        map[string]*Job
	Outbox      []*OutboxEvent
	ReadyQueues map[string][]string
	Delayed     []delayedItem
	Leases      map[string]*Lease
	DLQ         []string
	Published   map[string]struct{}
}

func NewInMemoryQueueForge() *InMemoryQueueForge {
	return &InMemoryQueueForge{
		nextID:      1,
		Now:         1_000,
		Paused:      false,
		Jobs:        make(map[string]*Job),
		Outbox:      make([]*OutboxEvent, 0),
		ReadyQueues: make(map[string][]string),
		Delayed:     make([]delayedItem, 0),
		Leases:      make(map[string]*Lease),
		DLQ:         make([]string, 0),
		Published:   make(map[string]struct{}),
	}
}

func (f *InMemoryQueueForge) CreateJob(input CreateJobInput) *Job {
	id := fmt.Sprintf("job-%d", f.nextID)
	f.nextID++

	scheduled := input.RunAt != nil && *input.RunAt > f.Now
	status := StatusPending
	if scheduled {
		status = StatusScheduled
	}

	maxAttempts := 3
	if input.MaxAttempts != nil {
		maxAttempts = *input.MaxAttempts
	}

	job := &Job{
		ID:          id,
		Name:        input.Name,
		Priority:    input.Priority,
		Status:      status,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		Version:     0,
		RunAt:       input.RunAt,
	}

	f.Jobs[id] = job

	if scheduled {
		dueAt := f.Now
		if input.RunAt != nil {
			dueAt = *input.RunAt
		}
		f.Delayed = append(f.Delayed, delayedItem{JobID: id, DueAt: dueAt})
	} else {
		f.Outbox = append(f.Outbox, &OutboxEvent{
			ID:               fmt.Sprintf("outbox-%s", id),
			JobID:            id,
			Priority:         input.Priority,
			Processed:        false,
			Attempts:         0,
			DeduplicationKey: fmt.Sprintf("job-created-%s", id),
		})
	}

	return job
}

func (f *InMemoryQueueForge) PublishNext(opts ...PublishOptions) int {
	if f.Paused {
		return 0
	}

	crashAfterPublish := false
	if len(opts) > 0 {
		crashAfterPublish = opts[0].CrashAfterPublish
	}

	var event *OutboxEvent
	for _, candidate := range f.Outbox {
		if !candidate.Processed {
			event = candidate
			break
		}
	}

	if event == nil {
		return 0
	}

	if _, exists := f.Published[event.DeduplicationKey]; !exists {
		f.enqueue(event.JobID, event.Priority)
		f.Published[event.DeduplicationKey] = struct{}{}
	}

	if crashAfterPublish {
		return 1
	}

	event.Processed = true
	return 1
}

func (f *InMemoryQueueForge) ProcessNext(handler func(job *Job) error, workerIDs ...string) error {
	workerID := "worker-1"
	if len(workerIDs) > 0 {
		workerID = workerIDs[0]
	}

	jobID := f.dequeue()
	if jobID == "" {
		return nil
	}

	lease := f.AcquireLease(jobID, workerID, 30_000)
	if lease == nil {
		return nil
	}

	return f.RunWithLease(jobID, lease, handler)
}

func (f *InMemoryQueueForge) AcquireLease(jobID, workerID string, ttlMs int64) *Lease {
	activeLease, ok := f.Leases[jobID]
	if ok && activeLease.ExpiresAt > f.Now {
		return nil
	}

	job, ok := f.Jobs[jobID]
	if !ok {
		return nil
	}

	lease := &Lease{
		JobID:        jobID,
		WorkerID:     workerID,
		FencingToken: job.Version + 1,
		ExpiresAt:    f.Now + ttlMs,
	}
	f.Leases[jobID] = lease
	return lease
}

func (f *InMemoryQueueForge) RunWithLease(jobID string, lease *Lease, handler func(job *Job) error) error {
	job, ok := f.Jobs[jobID]
	if !ok || !f.isLeaseCurrent(lease) {
		return nil
	}

	f.transition(job, StatusRunning)

	err := handler(job)
	if err == nil {
		if f.isLeaseCurrent(lease) {
			f.transition(job, StatusCompleted)
		}
	} else {
		if job.Attempts+1 >= job.MaxAttempts {
			job.Attempts++
			f.transition(job, StatusDeadLetter)
			f.DLQ = append(f.DLQ, job.ID)
		} else {
			job.Attempts++
			f.transition(job, StatusRetrying)
			delayMs := int64(5000.0 * math.Pow(2, float64(job.Attempts)))
			f.Delayed = append(f.Delayed, delayedItem{JobID: job.ID, DueAt: f.Now + delayMs})
		}
	}

	delete(f.Leases, jobID)
	return nil
}

func (f *InMemoryQueueForge) PromoteDelayed() int {
	promoted := 0
	var remaining []delayedItem

	for _, entry := range f.Delayed {
		if entry.DueAt <= f.Now {
			if job, ok := f.Jobs[entry.JobID]; ok {
				f.transition(job, StatusPending)
				f.enqueue(job.ID, job.Priority)
				promoted++
			}
		} else {
			remaining = append(remaining, entry)
		}
	}

	f.Delayed = remaining
	return promoted
}

func (f *InMemoryQueueForge) RecoverExpiredLeases() int {
	recovered := 0

	for jobID, lease := range f.Leases {
		job, ok := f.Jobs[jobID]
		if lease.ExpiresAt <= f.Now && ok && job.Status == StatusRunning {
			delete(f.Leases, jobID)
			f.transition(job, StatusPending)
			f.enqueue(job.ID, job.Priority)
			recovered++
		}
	}

	return recovered
}

func (f *InMemoryQueueForge) UpdateWithVersion(jobID string, nextStatus JobStatus, expectedVersion int) bool {
	job, ok := f.Jobs[jobID]
	if !ok || job.Version != expectedVersion {
		return false
	}
	if _, hasLease := f.Leases[jobID]; hasLease {
		return false
	}

	f.transition(job, nextStatus)
	return true
}

func (f *InMemoryQueueForge) enqueue(jobID string, priority int) {
	queue := "low"
	if priority >= 10 {
		queue = "high"
	} else if priority >= 5 {
		queue = "medium"
	}

	f.ReadyQueues[queue] = append(f.ReadyQueues[queue], jobID)
}

func (f *InMemoryQueueForge) dequeue() string {
	for _, queue := range []string{"high", "medium", "low"} {
		values := f.ReadyQueues[queue]
		if len(values) > 0 {
			jobID := values[0]
			f.ReadyQueues[queue] = values[1:]
			return jobID
		}
	}
	return ""
}

func (f *InMemoryQueueForge) transition(job *Job, status JobStatus) {
	job.Status = status
	job.Version++
}

func (f *InMemoryQueueForge) isLeaseCurrent(lease *Lease) bool {
	activeLease, ok := f.Leases[lease.JobID]
	return ok && activeLease.FencingToken == lease.FencingToken && activeLease.ExpiresAt > f.Now
}

// ---------------------- 7 End-to-End Test Scenarios ----------------------

func TestEndToEndJobLifecycle(t *testing.T) {
	forge := NewInMemoryQueueForge()
	job := forge.CreateJob(CreateJobInput{Name: "send-email", Priority: 10})

	if len(forge.Outbox) != 1 {
		t.Fatalf("expected 1 outbox event, got %d", len(forge.Outbox))
	}
	if published := forge.PublishNext(); published != 1 {
		t.Fatalf("expected 1 published event, got %d", published)
	}

	_ = forge.ProcessNext(func(j *Job) error {
		return nil
	})

	if forge.Jobs[job.ID].Status != StatusCompleted {
		t.Errorf("expected COMPLETED status, got %s", forge.Jobs[job.ID].Status)
	}
}

func TestRetryAndDLQ(t *testing.T) {
	forge := NewInMemoryQueueForge()
	maxAttempts := 2
	job := forge.CreateJob(CreateJobInput{Name: "always-fails", Priority: 5, MaxAttempts: &maxAttempts})
	forge.PublishNext()

	// 1st attempt fails -> retry scheduled
	_ = forge.ProcessNext(func(j *Job) error {
		return fmt.Errorf("boom")
	})

	forge.Now += 20_000
	forge.PromoteDelayed()

	// 2nd attempt fails -> max attempts reached, moved to DLQ
	_ = forge.ProcessNext(func(j *Job) error {
		return fmt.Errorf("boom")
	})

	if forge.Jobs[job.ID].Status != StatusDeadLetter {
		t.Errorf("expected DEAD_LETTER status, got %s", forge.Jobs[job.ID].Status)
	}
	if len(forge.DLQ) != 1 || forge.DLQ[0] != job.ID {
		t.Errorf("expected job in DLQ, got %v", forge.DLQ)
	}
}

func TestWorkerCrashLeaseRecovery(t *testing.T) {
	forge := NewInMemoryQueueForge()
	job := forge.CreateJob(CreateJobInput{Name: "long-task", Priority: 10})
	forge.PublishNext()

	lease := forge.AcquireLease(job.ID, "worker-1", 100)
	if lease == nil {
		t.Fatal("expected lease to be acquired")
	}
	forge.Jobs[job.ID].Status = StatusRunning
	forge.Now += 101

	if recovered := forge.RecoverExpiredLeases(); recovered != 1 {
		t.Fatalf("expected 1 recovered lease, got %d", recovered)
	}

	_ = forge.ProcessNext(func(j *Job) error {
		return nil
	}, "worker-2")

	if forge.Jobs[job.ID].Status != StatusCompleted {
		t.Errorf("expected COMPLETED status after recovery, got %s", forge.Jobs[job.ID].Status)
	}
}

func TestPublisherCrashIdempotency(t *testing.T) {
	forge := NewInMemoryQueueForge()
	job := forge.CreateJob(CreateJobInput{Name: "publish-once", Priority: 10})

	forge.PublishNext(PublishOptions{CrashAfterPublish: true})
	forge.PublishNext()

	if !forge.Outbox[0].Processed {
		t.Error("expected outbox event to be marked processed")
	}

	highQueue := forge.ReadyQueues["high"]
	if len(highQueue) != 1 || highQueue[0] != job.ID {
		t.Errorf("expected exactly [job.id] in high queue, got %v", highQueue)
	}
}

func TestDelayedJobPromotion(t *testing.T) {
	forge := NewInMemoryQueueForge()
	runAt := forge.Now + 1_000
	job := forge.CreateJob(CreateJobInput{Name: "scheduled", Priority: 1, RunAt: &runAt})

	if promoted := forge.PromoteDelayed(); promoted != 0 {
		t.Fatalf("expected 0 promoted before time, got %d", promoted)
	}

	forge.Now += 1_001
	if promoted := forge.PromoteDelayed(); promoted != 1 {
		t.Fatalf("expected 1 promoted after delay elapsed, got %d", promoted)
	}

	_ = forge.ProcessNext(func(j *Job) error {
		return nil
	})

	if forge.Jobs[job.ID].Status != StatusCompleted {
		t.Errorf("expected COMPLETED status, got %s", forge.Jobs[job.ID].Status)
	}
}

func TestQueuePausePreventsPublishing(t *testing.T) {
	forge := NewInMemoryQueueForge()
	job := forge.CreateJob(CreateJobInput{Name: "pause-me", Priority: 5})

	forge.Paused = true
	if published := forge.PublishNext(); published != 0 {
		t.Fatalf("expected 0 published when paused, got %d", published)
	}

	forge.Paused = false
	if published := forge.PublishNext(); published != 1 {
		t.Fatalf("expected 1 published after resume, got %d", published)
	}

	medQueue := forge.ReadyQueues["medium"]
	if len(medQueue) != 1 || medQueue[0] != job.ID {
		t.Errorf("expected job in medium queue, got %v", medQueue)
	}
}

func TestConcurrentVersionedCommand(t *testing.T) {
	forge := NewInMemoryQueueForge()
	job := forge.CreateJob(CreateJobInput{Name: "race", Priority: 5})

	retryAccepted := forge.UpdateWithVersion(job.ID, StatusPending, job.Version)
	cancelAccepted := forge.UpdateWithVersion(job.ID, StatusCanceled, job.Version-1)

	if !retryAccepted {
		t.Error("expected retry with current version to be accepted")
	}
	if cancelAccepted {
		t.Error("expected cancel with stale version to be rejected")
	}
}

