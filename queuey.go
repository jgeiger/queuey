/*
Package queuey provides queue that processes based on
FIFO and locks the queue from futher reads until manually
unlocked.
*/
package queuey

import (
	"errors"
	"sync"
)

type (
	// Queue contains the priorityQueue and messagePacks map.
	Queue struct {
		sync.Mutex
		priorityQueue []string
		messagePacks  map[string]*MessagePack
	}

	// MessagePack is a container for the messages from the queue.
	MessagePack struct {
		Key          string
		Messages     []string
		MessageCount int
		locked       bool
	}
)

/*
New returns a new Queue containing a priorityQueue,
a map of MessagePacks and a Mutex.

Inputs:
None

Outputs:
*Queue
*/
func New() *Queue {
	return &Queue{
		messagePacks: make(map[string]*MessagePack),
	}
}

/*
Push adds the supplied message to the end of the map element
referenced by the mapKey. It also adds the mapKey to the end
of the priorityQueue if it does not already exist.

Inputs:
mapKey: string
message: string

Outputs:
None
*/
func (q *Queue) Push(mapKey string, message string) {
	q.Lock()
	defer q.Unlock()
	if _, ok := q.messagePacks[mapKey]; !ok {
		q.priorityQueue = append(q.priorityQueue, mapKey)
		q.messagePacks[mapKey] = &MessagePack{Key: mapKey, locked: false}
	}
	q.messagePacks[mapKey].Messages = append(q.messagePacks[mapKey].Messages, message)
}

/*
Pop returns the MessagePack referenced by the next mapKey
pulled from the priorityQueue.

Inputs:
None

Outputs:
MessagePack
*/
func (q *Queue) Pop() MessagePack {
	q.Lock()
	defer q.Unlock()
	mapKey := getNextPriority(q)
	messagePack, err := getNextMessagePack(q, mapKey)
	if err != nil {
		return MessagePack{}
	}
	return *messagePack
}

/*
ClearMessagePackLock removes the lock on the map item
referenced by mapKey. It also adds that mapKey back into
the priorityQueue.

Inputs:
mapKey: string

Outputs:
None
*/
func (q *Queue) ClearMessagePackLock(mapKey string) {
	q.Lock()
	defer q.Unlock()
	messagePack := q.messagePacks[mapKey]
	messagePack.Messages = messagePack.Messages[messagePack.MessageCount:]
	messagePack.MessageCount = 0
	messagePack.locked = false
	q.priorityQueue = append(q.priorityQueue, mapKey)
}

func getNextPriority(q *Queue) string {
	switch len(q.priorityQueue) {
	case 0:
		return ""
	case 1:
		mapKey := q.priorityQueue[0]
		q.priorityQueue = nil
		return mapKey
	}
	mapKey := q.priorityQueue[0]
	q.priorityQueue = q.priorityQueue[1:]
	return mapKey
}

func getNextMessagePack(q *Queue, mapKey string) (*MessagePack, error) {
	if mapKey != "" {
		messagePack := q.messagePacks[mapKey]
		messagePack.locked = true
		messagePack.MessageCount = len(messagePack.Messages)
		return messagePack, nil
	}
	return nil, errors.New("No valid messagePack")
}
