package queue

import (
	"container/heap"
	"sync"
	"time"

	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
)

type PriorityQueueItem struct {
	Task       *pb.Task
	Priority   int
	Index      int
	EnqueuedAt time.Time
}

// PriorityQueue — куча (heap) с индексом по task ID для быстрого поиска и удаления.
// Используем container/heap, который реализует min-heap, но с Less反转ым — получаем max-heap по приоритету.
type PriorityQueue struct {
	items []*PriorityQueueItem
	index map[string]*PriorityQueueItem
	mu    sync.RWMutex
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		items: make([]*PriorityQueueItem, 0),
		index: make(map[string]*PriorityQueueItem),
	}
	heap.Init(pq)
	return pq
}

func (pq *PriorityQueue) Len() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return len(pq.items)
}

// Less — приоритет выше = раньше. При одинаковом приоритете — кто раньше добавлен, тот раньше.
func (pq *PriorityQueue) Less(i, j int) bool {
	if pq.items[i].Priority != pq.items[j].Priority {
		return pq.items[i].Priority > pq.items[j].Priority
	}
	return pq.items[i].EnqueuedAt.Before(pq.items[j].EnqueuedAt)
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*PriorityQueueItem)
	item.Index = len(pq.items)
	pq.items = append(pq.items, item)
	pq.index[item.Task.Id] = item
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	pq.items = old[:n-1]
	delete(pq.index, item.Task.Id)
	return item
}

func (pq *PriorityQueue) Enqueue(task *pb.Task) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if _, exists := pq.index[task.Id]; exists {
		return
	}

	item := &PriorityQueueItem{
		Task:       task,
		Priority:   int(task.Priority),
		EnqueuedAt: time.Now(),
	}

	heap.Push(pq, item)
}

func (pq *PriorityQueue) Dequeue() *pb.Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil
	}

	item := heap.Pop(pq).(*PriorityQueueItem)
	return item.Task
}

func (pq *PriorityQueue) Peek() *pb.Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if len(pq.items) == 0 {
		return nil
	}

	return pq.items[0].Task
}

func (pq *PriorityQueue) Remove(taskID string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item, exists := pq.index[taskID]
	if !exists {
		return false
	}

	heap.Remove(pq, item.Index)
	return true
}

func (pq *PriorityQueue) Contains(taskID string) bool {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	_, exists := pq.index[taskID]
	return exists
}

func (pq *PriorityQueue) UpdatePriority(taskID string, newPriority int) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item, exists := pq.index[taskID]
	if !exists {
		return false
	}

	item.Priority = newPriority
	heap.Fix(pq, item.Index)
	return true
}

func (pq *PriorityQueue) Tasks() []*pb.Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	tasks := make([]*pb.Task, 0, len(pq.items))
	for _, item := range pq.items {
		tasks = append(tasks, item.Task)
	}
	return tasks
}
