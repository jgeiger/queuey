package queuey

import "testing"

func NewOrFatalQueue(t *testing.T) *Queue {
	q := New()
	return q
}

func TestNewQueue(t *testing.T) {
	q := NewOrFatalQueue(t)
	if q.messagePacks == nil {
		t.Errorf("Expected messagePacks, got %v", q.messagePacks)
	}
}

func TestPush(t *testing.T) {
	q := NewOrFatalQueue(t)
	key1 := "one"
	key2 := "two"
	q.Push(key1, "message")

	if q.StoredMessages != 1 {
		t.Errorf("Expected one message, got %v", q.StoredMessages)
	}

	if q.LockedMessagePacks != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedMessagePacks)
	}

	if len(q.messagePacks) != 1 {
		t.Errorf("Expected one key, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 1 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}

	q.Push(key1, "message")
	if len(q.messagePacks) != 1 {
		t.Errorf("Expected one key, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 2 {
		t.Errorf("Expected two messagePacks, got %v", len(q.messagePacks[key1].Messages))
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}

	q.Push(key2, "message")
	if len(q.messagePacks) != 2 {
		t.Errorf("Expected two keys, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key2].Messages) != 1 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key2].Messages))
	}

	if len(q.priorityQueue) != 2 {
		t.Errorf("Expected two queued items, got %v", len(q.priorityQueue))
	}
}

func TestPop(t *testing.T) {
	q := NewOrFatalQueue(t)
	key1 := "one"
	key2 := "two"
	q.Push(key1, "message")
	q.Push(key1, "message")
	q.Push(key2, "message")

	q.Pop()

	if q.StoredMessages != 3 {
		t.Errorf("Expected three messages, got %v", q.StoredMessages)
	}

	if q.LockedMessagePacks != 1 {
		t.Errorf("Expected one locked message pack, got %v", q.LockedMessagePacks)
	}

	if len(q.messagePacks) != 2 {
		t.Errorf("Expected two keys, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 2 {
		t.Errorf("Expected two message, got %v", len(q.messagePacks[key1].Messages))
	}

	if q.messagePacks[key1].LockedAt == 0 {
		t.Errorf("Expected locked to be non-zero, got %v", q.messagePacks[key1].LockedAt)
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}
}

func TestClearLockNoMoreMessages(t *testing.T) {
	q := NewOrFatalQueue(t)
	key1 := "one"
	q.Push(key1, "message")
	mp, _ := q.Pop()
	q.ClearLock(key1, mp.LockedAt)

	if q.StoredMessages != 0 {
		t.Errorf("Expected zero messages, got %v", q.StoredMessages)
	}

	if q.LockedMessagePacks != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedMessagePacks)
	}

	if len(q.messagePacks) != 0 {
		t.Errorf("Expected zero keys, got %v", len(q.messagePacks))
	}

	if len(q.priorityQueue) != 0 {
		t.Errorf("Expected zero queued items, got %v", len(q.priorityQueue))
	}
}

func TestClearLockMoreItemsSingleQueue(t *testing.T) {
	q := NewOrFatalQueue(t)
	key1 := "one"
	q.Push(key1, "message")
	q.Push(key1, "message")
	mp, _ := q.Pop()
	q.Push(key1, "message")
	q.ClearLock(key1, mp.LockedAt)

	if q.StoredMessages != 1 {
		t.Errorf("Expected one message, got %v", q.StoredMessages)
	}

	if q.LockedMessagePacks != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedMessagePacks)
	}

	if len(q.messagePacks) != 1 {
		t.Errorf("Expected one key, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 1 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if q.messagePacks[key1].LockedAt != 0 {
		t.Errorf("Expected locked to be zero, got %v", q.messagePacks[key1].LockedAt)
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}
}

func TestClearLockMoreItemsSingleQueueNonMatchingLockedAt(t *testing.T) {
	q := NewOrFatalQueue(t)
	key1 := "one"
	q.Push(key1, "message")
	q.Push(key1, "message")
	_, _ = q.Pop()
	q.Push(key1, "message")
	q.ClearLock(key1, 1234)

	if q.StoredMessages != 3 {
		t.Errorf("Expected three messages, got %v", q.StoredMessages)
	}

	if q.LockedMessagePacks != 1 {
		t.Errorf("Expected one locked message pack, got %v", q.LockedMessagePacks)
	}

	if len(q.messagePacks) != 1 {
		t.Errorf("Expected one key, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 3 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if q.messagePacks[key1].LockedAt == 0 {
		t.Errorf("Expected locked to be non-zero, got %v", q.messagePacks[key1].LockedAt)
	}

	if len(q.priorityQueue) != 0 {
		t.Errorf("Expected zero queued items, got %v", len(q.priorityQueue))
	}
}

func TestClearLockMultipleKeys(t *testing.T) {
	q := NewOrFatalQueue(t)
	key1 := "one"
	key2 := "two"
	q.Push(key1, "message")
	q.Push(key1, "message")
	q.Push(key2, "message")
	mp, _ := q.Pop()
	q.Push(key1, "message")
	q.ClearLock(key1, mp.LockedAt)

	if q.StoredMessages != 2 {
		t.Errorf("Expected one message, got %v", q.StoredMessages)
	}

	if q.LockedMessagePacks != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedMessagePacks)
	}

	if len(q.messagePacks) != 2 {
		t.Errorf("Expected two keys, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 1 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if q.messagePacks[key1].LockedAt != 0 {
		t.Errorf("Expected locked to be zero, got %v", q.messagePacks[key1].LockedAt)
	}

	if len(q.priorityQueue) != 2 {
		t.Errorf("Expected two queued items, got %v", len(q.priorityQueue))
	}
}

func BenchmarkPush(b *testing.B) {
	// run the Push function b.N times
	q := New()
	for n := 0; n < b.N; n++ {
		q.Push("abcd", "message")
	}
}

func BenchmarkPop(b *testing.B) {
	// run the Pop function b.N times
	q := New()
	for n := 0; n < b.N; n++ {
		q.Push("abcd", "message")
		_, _ = q.Pop()
	}
}

func BenchmarkClearLockMatchingLock(b *testing.B) {
	// run the ClearLock function b.N times
	q := New()
	for n := 0; n < b.N; n++ {
		q.Push("abcd", "message")
		mp, _ := q.Pop()
		q.ClearLock("abcd", mp.LockedAt)
	}
}

func BenchmarkClearLockNonMatchingLock(b *testing.B) {
	// run the ClearLock function b.N times
	q := New()
	for n := 0; n < b.N; n++ {
		q.Push("abcd", "message")
		_, _ = q.Pop()
		q.ClearLock("abcd", 1234)
	}
}
