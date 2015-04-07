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
		MessageCount int64
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
	if _, ok := q.messagePacks[mapKey]; !ok {
		q.priorityQueue = append(q.priorityQueue, mapKey)
		q.messagePacks[mapKey] = &MessagePack{Key: mapKey, locked: false}
	}
	q.messagePacks[mapKey].Messages = append(q.messagePacks[mapKey].Messages, message)
	q.Unlock()
}

/*
Pop returns the MessagePack referenced by the next mapKey
pulled from the priorityQueue.

Inputs:
None

Outputs:
MessagePack
*/
func (q *Queue) Pop() (MessagePack, error) {
	q.Lock()
	messagePack, err := getNextMessagePack(q)
	if err != nil {
		q.Unlock()
		return MessagePack{}, err
	}
	q.Unlock()
	return *messagePack, nil
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
	messagePack := q.messagePacks[mapKey]
	messagePack.Messages = messagePack.Messages[messagePack.MessageCount:]
	if len(messagePack.Messages) == 0 {
		delete(q.messagePacks, mapKey)
	} else {
		messagePack.locked = false
		q.priorityQueue = append(q.priorityQueue, mapKey)
	}
	q.Unlock()
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

func getNextMessagePack(q *Queue) (*MessagePack, error) {
	if mapKey := getNextPriority(q); mapKey != "" {
		messagePack := q.messagePacks[mapKey]
		messagePack.locked = true
		messagePack.MessageCount = int64(len(messagePack.Messages))
		return messagePack, nil
	}
	return nil, errors.New("No valid messagePack")
}
