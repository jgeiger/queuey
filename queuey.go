/*
Package queuey provides queue that processes based on
FIFO and locks the queue from futher reads until manually
unlocked.
*/
package queuey

import (
	"errors"
	"sync"
	"time"
)

type (
	// Queue contains the priorityQueue and messagePacks map.
	Queue struct {
		sync.Mutex
		priorityQueue      []string
		messagePacks       map[string]*MessagePack
		LockedMessagePacks int64
		StoredMessages     int64
	}

	// MessagePack is a container for the messages from the queue.
	MessagePack struct {
		Key          string
		Messages     []string
		MessageCount int64
		LockedAt     int
		queue        *Queue
		unlockTimer  *time.Timer
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
		messagePacks:       make(map[string]*MessagePack),
		LockedMessagePacks: 0,
		StoredMessages:     0,
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
		q.messagePacks[mapKey] = &MessagePack{Key: mapKey, queue: q}
	}
	q.messagePacks[mapKey].Messages = append(q.messagePacks[mapKey].Messages, message)
	q.StoredMessages = q.StoredMessages + 1
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
	mp, err := getNextMessagePack(q)
	if err != nil {
		q.Unlock()
		return MessagePack{}, err
	}
	q.Unlock()
	return *mp, nil
}

/*
ClearLock removes the lock on the map item
referenced by mapKey. It also adds that mapKey back into
the priorityQueue.

Inputs:
mapKey: string

Outputs:
None
*/
func (q *Queue) ClearLock(mapKey string, locked int) {
	q.Lock()
	mp := q.messagePacks[mapKey]
	if locked == mp.LockedAt {
		mp.unlockTimer.Stop()
		mp.Messages = mp.Messages[mp.MessageCount:]
		mp.LockedAt = 0
		q.StoredMessages = q.StoredMessages - mp.MessageCount
		q.LockedMessagePacks = q.LockedMessagePacks - 1

		if len(mp.Messages) == 0 {
			delete(q.messagePacks, mapKey)
		} else {
			q.priorityQueue = append(q.priorityQueue, mapKey)
		}
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
		mp := q.messagePacks[mapKey]
		mp.MessageCount = int64(len(mp.Messages))
		mp.unlockTimer = time.NewTimer(15 * time.Second)
		mp.LockedAt = time.Now().Nanosecond()
		q.LockedMessagePacks = q.LockedMessagePacks + 1
		go mp.timeoutMessagepack()
		return mp, nil
	}
	return nil, errors.New("No valid messagePack")
}

func (mp *MessagePack) timeoutMessagepack() {
	<-mp.unlockTimer.C
	mp.MessageCount = 0
	mp.queue.ClearLock(mp.Key, mp.LockedAt)
}
