package redisqueue

// Queue name constants matching TS QUEUES.
const (
	QueueHigh    = "queue:high"
	QueueMedium  = "queue:medium"
	QueueLow     = "queue:low"
	QueueDelayed = "queue:delayed"
)

// QueuePriority enum values matching TS QueuePriority.
const (
	PriorityLow    = 1
	PriorityMedium = 5
	PriorityHigh   = 10
)

// ResolveQueue maps numeric priority to Redis queue list name.
func ResolveQueue(priority int) string {
	if priority >= PriorityHigh {
		return QueueHigh
	}
	if priority >= PriorityMedium {
		return QueueMedium
	}
	return QueueLow
}

