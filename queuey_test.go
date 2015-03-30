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
	if len(q.messagePacks) != 2 {
		t.Errorf("Expected two keys, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 2 {
		t.Errorf("Expected two message, got %v", len(q.messagePacks[key1].Messages))
	}

	if q.messagePacks[key1].locked != true {
		t.Errorf("Expected locked to be true, got %v", q.messagePacks[key1].locked)
	}

	if len(q.priorityQueue) != 1 {
		t.Errorf("Expected one queued item, got %v", len(q.priorityQueue))
	}
}

func TestClearLock(t *testing.T) {
	q := NewOrFatalQueue(t)
	key1 := "one"
	key2 := "two"
	q.Push(key1, "message")
	q.Push(key1, "message")
	q.Push(key2, "message")
	_ = q.Pop()
	q.Push(key1, "message")
	q.ClearMessagePackLock(key1)

	if len(q.messagePacks) != 2 {
		t.Errorf("Expected two keys, got %v", len(q.messagePacks))
	}

	if len(q.messagePacks[key1].Messages) != 1 {
		t.Errorf("Expected one message, got %v", len(q.messagePacks[key1].Messages))
	}

	if q.messagePacks[key1].locked != false {
		t.Errorf("Expected locked to be false, got %v", q.messagePacks[key1].locked)
	}

	if len(q.priorityQueue) != 2 {
		t.Errorf("Expected two queued items, got %v", len(q.priorityQueue))
	}
}
