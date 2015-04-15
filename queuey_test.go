package queuey

import "testing"

const key1 = "one"
const key2 = "two"

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

func TestExpireLocksBeforeTimeout(t *testing.T) {
	q := NewOrFatalQueue(t)
	m := []byte("message")
	q.Push(key1, m)
	q.Pop()

	q.expireLocks()

	if q.LockedCount() != 1 {
		t.Errorf("Expected one locked messagePacks, got %v", q.LockedCount())
	}
}

func TestExpireLocksAfterTimeout(t *testing.T) {
	q := NewOrFatalQueue(t)
	m := []byte("message")
	q.Push(key1, m)
	q.Pop()
	q.lockedPacks[key1] = 1

	q.expireLocks()

	if q.LockedCount() != 0 {
		t.Errorf("Expected zero locked messagePacks, got %v", q.LockedCount())
	}
}

func TestPush(t *testing.T) {
	q := NewOrFatalQueue(t)
	m := []byte("message")
	q.Push(key1, m)

	if q.StoredMessages != 1 {
		t.Errorf("Expected one message, got %v", q.StoredMessages)
	}

	if q.LockedCount() != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedCount())
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

	q.Push(key1, m)
	if len(q.messagePacks) != 1 {
		t.Errorf("Expected one key, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 2 {
		t.Errorf("Expected two messagePacks, got %v", len(q.messagePacks[key1].Messages))
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}

	q.Push(key2, m)
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
	m := []byte("message")
	q.Push(key1, m)
	q.Push(key1, m)
	q.Push(key2, m)

	q.Pop()

	if q.StoredMessages != 3 {
		t.Errorf("Expected three messages, got %v", q.StoredMessages)
	}

	if q.LockedCount() != 1 {
		t.Errorf("Expected one locked message pack, got %v", q.LockedCount())
	}

	if len(q.messagePacks) != 2 {
		t.Errorf("Expected two keys, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 2 {
		t.Errorf("Expected two message, got %v", len(q.messagePacks[key1].Messages))
	}

	if q.lockedPacks[key1] == 0 {
		t.Errorf("Expected locked to be non-zero, got %v", q.lockedPacks[key1])
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}
}

func TestClearLockNoMoreMessages(t *testing.T) {
	q := NewOrFatalQueue(t)
	m := []byte("message")
	q.Push(key1, m)
	_, _ = q.Pop()
	p := ClearParams{ID: key1, LockedAt: q.lockedPacks[key1], AlreadyLocked: false}
	q.ClearLock(p)

	if q.StoredMessages != 0 {
		t.Errorf("Expected zero messages, got %v", q.StoredMessages)
	}

	if q.LockedCount() != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedCount())
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
	m := []byte("message")
	q.Push(key1, m)
	q.Push(key1, m)
	_, _ = q.Pop()
	q.Push(key1, m)
	p := ClearParams{ID: key1, LockedAt: q.lockedPacks[key1], AlreadyLocked: false}
	q.ClearLock(p)

	if q.StoredMessages != 1 {
		t.Errorf("Expected one message, got %v", q.StoredMessages)
	}

	if q.LockedCount() != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedCount())
	}

	if len(q.messagePacks) != 1 {
		t.Errorf("Expected one key, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 1 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if _, ok := q.lockedPacks[key1]; ok {
		t.Errorf("Expected no locked time, got %v", q.lockedPacks[key1])
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}
}

func TestClearLockMoreItemsSingleQueueNonMatchingLockedAt(t *testing.T) {
	q := NewOrFatalQueue(t)
	m := []byte("message")
	q.Push(key1, m)
	q.Push(key1, m)
	_, _ = q.Pop()
	q.Push(key1, m)
	p := ClearParams{ID: key1, LockedAt: 1234, AlreadyLocked: false}
	q.ClearLock(p)

	if q.StoredMessages != 3 {
		t.Errorf("Expected three messages, got %v", q.StoredMessages)
	}

	if q.LockedCount() != 1 {
		t.Errorf("Expected one locked message pack, got %v", q.LockedCount())
	}

	if len(q.messagePacks) != 1 {
		t.Errorf("Expected one key, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 3 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if lt, ok := q.lockedPacks[key1]; !ok {
		t.Errorf("Expected locked time, got %v", lt)
	}

	if len(q.priorityQueue) != 0 {
		t.Errorf("Expected zero queued items, got %v", len(q.priorityQueue))
	}
}

func TestClearLockMultipleKeys(t *testing.T) {
	q := NewOrFatalQueue(t)
	m := []byte("message")
	q.Push(key1, m)
	q.Push(key1, m)
	q.Push(key2, m)
	_, _ = q.Pop()
	q.Push(key1, m)
	p := ClearParams{ID: key1, LockedAt: q.lockedPacks[key1], AlreadyLocked: false}
	q.ClearLock(p)

	if q.StoredMessages != 2 {
		t.Errorf("Expected one message, got %v", q.StoredMessages)
	}

	if q.LockedCount() != 0 {
		t.Errorf("Expected zero locked message packs, got %v", q.LockedCount())
	}

	if len(q.messagePacks) != 2 {
		t.Errorf("Expected two keys, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 1 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if lt, ok := q.lockedPacks[key1]; ok {
		t.Errorf("Expected no locked time, got %v", lt)
	}

	if len(q.priorityQueue) != 2 {
		t.Errorf("Expected two queued items, got %v", len(q.priorityQueue))
	}
}

func BenchmarkPush(b *testing.B) {
	// run the Push function b.N times
	q := New()
	m := []byte("message")
	for n := 0; n < b.N; n++ {
		q.Push(key1, m)
	}
}

func BenchmarkPop(b *testing.B) {
	// run the Pop function b.N times
	q := New()
	m := []byte("message")
	for n := 0; n < b.N; n++ {
		q.Push(key1, m)
		_, _ = q.Pop()
	}
}

func BenchmarkClearLockMatchingLock(b *testing.B) {
	// run the ClearLock function b.N times
	q := New()
	m := []byte("message")
	for n := 0; n < b.N; n++ {
		q.Push(key1, m)
		_, _ = q.Pop()
		p := ClearParams{ID: key1, LockedAt: q.lockedPacks[key1], AlreadyLocked: false}
		q.ClearLock(p)
	}
}

func BenchmarkClearLockNonMatchingLock(b *testing.B) {
	// run the ClearLock function b.N times
	q := New()
	m := []byte("message")
	for n := 0; n < b.N; n++ {
		q.Push(key1, m)
		_, _ = q.Pop()
		p := ClearParams{ID: key1, LockedAt: 1234, AlreadyLocked: false}
		q.ClearLock(p)
	}
}

func BenchmarkLockedCount(b *testing.B) {
	// run the ClearLock function b.N times
	q := New()
	m := []byte("message")
	q.Push(key1, m)
	_, _ = q.Pop()
	for n := 0; n < b.N; n++ {
		q.LockedCount()
	}
}
