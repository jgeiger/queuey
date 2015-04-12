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
		priorityQueue  []string
		messagePacks   map[string]*MessagePack
		lockedPacks    map[string]int64
		StoredMessages int64
	}

	// MessagePack is a container for the messages from the queue.
	MessagePack struct {
		Key          string
		Messages     []string
		MessageCount int64
		LockedAt     int64
	}

	// ClearParams contains the values passed to ClearLock
	ClearParams struct {
		ID            string
		LockedAt      int64
		AlreadyLocked bool
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
	q := &Queue{
		messagePacks:   make(map[string]*MessagePack),
		lockedPacks:    make(map[string]int64),
		StoredMessages: 0,
	}
	go q.lockTicker()
	return q
}

func (q *Queue) lockTicker() {
	t := time.NewTicker(15 * time.Second).C

	for {
		select {
		case <-t:
			q.expireLocks()
		}
	}
}

func (q *Queue) expireLocks() {
	ago := time.Now().UnixNano() - (15 * time.Second).Nanoseconds()
	for k, v := range q.lockedPacks {
		if v < ago {
			q.Lock()
			q.messagePacks[k].MessageCount = 0
			q.ClearLock(ClearParams{ID: k, LockedAt: v, AlreadyLocked: true})
			delete(q.lockedPacks, k)
			q.Unlock()
		}
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
		q.messagePacks[mapKey] = &MessagePack{Key: mapKey}
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
func (q *Queue) ClearLock(p ClearParams) {
	if !p.AlreadyLocked {
		q.Lock()
	}
	if lockTime, ok := q.lockedPacks[p.ID]; ok && p.LockedAt == lockTime {
		mp := q.messagePacks[p.ID]
		mp.Messages = mp.Messages[mp.MessageCount:]
		delete(q.lockedPacks, p.ID)
		mp.LockedAt = 0
		q.StoredMessages = q.StoredMessages - mp.MessageCount

		if len(mp.Messages) == 0 {
			delete(q.messagePacks, p.ID)
		} else {
			q.priorityQueue = append(q.priorityQueue, p.ID)
		}
	}
	if !p.AlreadyLocked {
		q.Unlock()
	}
}

/*
LockedCount returns the number of locked MessagePacks

Inputs:
None

Outputs:
int
*/
func (q *Queue) LockedCount() int {
	return len(q.lockedPacks)
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
		now := time.Now().UnixNano()
		mp.LockedAt = now
		q.lockedPacks[mapKey] = now
		return mp, nil
	}
	return nil, errors.New("No valid messagePack")
}
