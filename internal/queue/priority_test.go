package queue

import (
	"testing"

	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/google/uuid"
)

func TestPriorityQueueEnqueueDequeue(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "low-priority",
		Priority: pb.TaskPriority_TASK_PRIORITY_LOW,
	}

	task2 := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "high-priority",
		Priority: pb.TaskPriority_TASK_PRIORITY_HIGH,
	}

	pq.Enqueue(task1)
	pq.Enqueue(task2)

	result := pq.Dequeue()
	if result == nil {
		t.Fatal("Expected task, got nil")
	}

	if result.Name != "high-priority" {
		t.Errorf("Expected high-priority task first, got %s", result.Name)
	}
}

func TestPriorityQueueEmpty(t *testing.T) {
	pq := NewPriorityQueue()

	result := pq.Dequeue()
	if result != nil {
		t.Errorf("Expected nil, got task %s", result.Id)
	}
}

func TestPriorityQueuePeek(t *testing.T) {
	pq := NewPriorityQueue()

	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	pq.Enqueue(task)

	peeked := pq.Peek()
	if peeked == nil {
		t.Fatal("Expected task, got nil")
	}

	if peeked.Name != "test-task" {
		t.Errorf("Expected test-task, got %s", peeked.Name)
	}

	// Queue should still have the item
	if pq.Len() != 1 {
		t.Errorf("Expected queue length 1 after peek, got %d", pq.Len())
	}
}

func TestPriorityQueueRemove(t *testing.T) {
	pq := NewPriorityQueue()

	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	pq.Enqueue(task)

	removed := pq.Remove(task.Id)
	if !removed {
		t.Fatal("Expected task to be removed")
	}

	if pq.Len() != 0 {
		t.Errorf("Expected queue length 0 after remove, got %d", pq.Len())
	}
}

func TestPriorityQueueRemoveNonExistent(t *testing.T) {
	pq := NewPriorityQueue()

	removed := pq.Remove(uuid.New().String())
	if removed {
		t.Fatal("Expected false for non-existent task")
	}
}

func TestPriorityQueueContains(t *testing.T) {
	pq := NewPriorityQueue()

	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	pq.Enqueue(task)

	if !pq.Contains(task.Id) {
		t.Fatal("Expected queue to contain task")
	}
}

func TestPriorityQueueNotContains(t *testing.T) {
	pq := NewPriorityQueue()

	if pq.Contains(uuid.New().String()) {
		t.Fatal("Expected queue to not contain non-existent task")
	}
}

func TestPriorityQueueUpdatePriority(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "task1",
		Priority: pb.TaskPriority_TASK_PRIORITY_LOW,
	}

	task2 := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "task2",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	pq.Enqueue(task1)
	pq.Enqueue(task2)

	// task2 should be first
	result := pq.Dequeue()
	if result.Name != "task2" {
		t.Errorf("Expected task2 first, got %s", result.Name)
	}

	// Re-enqueue task1 with higher priority
	pq.Enqueue(task1)
	pq.UpdatePriority(task1.Id, int(pb.TaskPriority_TASK_PRIORITY_CRITICAL))

	result = pq.Dequeue()
	if result.Name != "task1" {
		t.Errorf("Expected task1 first after priority update, got %s", result.Name)
	}
}

func TestPriorityQueueDuplicateEnqueue(t *testing.T) {
	pq := NewPriorityQueue()

	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	pq.Enqueue(task)
	pq.Enqueue(task) // Duplicate

	if pq.Len() != 1 {
		t.Errorf("Expected queue length 1, got %d", pq.Len())
	}
}

func TestPriorityQueueTasks(t *testing.T) {
	pq := NewPriorityQueue()

	for i := 0; i < 5; i++ {
		pq.Enqueue(&pb.Task{
			Id:       uuid.New().String(),
			Name:     "task",
			Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
		})
	}

	tasks := pq.Tasks()
	if len(tasks) != 5 {
		t.Errorf("Expected 5 tasks, got %d", len(tasks))
	}
}

func TestPriorityQueueMultiplePriorities(t *testing.T) {
	pq := NewPriorityQueue()

	priorities := []pb.TaskPriority{
		pb.TaskPriority_TASK_PRIORITY_LOW,
		pb.TaskPriority_TASK_PRIORITY_CRITICAL,
		pb.TaskPriority_TASK_PRIORITY_NORMAL,
		pb.TaskPriority_TASK_PRIORITY_HIGH,
	}

	for _, p := range priorities {
		pq.Enqueue(&pb.Task{
			Id:       uuid.New().String(),
			Name:     "task",
			Priority: p,
		})
	}

	expected := []pb.TaskPriority{
		pb.TaskPriority_TASK_PRIORITY_CRITICAL,
		pb.TaskPriority_TASK_PRIORITY_HIGH,
		pb.TaskPriority_TASK_PRIORITY_NORMAL,
		pb.TaskPriority_TASK_PRIORITY_LOW,
	}

	for _, exp := range expected {
		result := pq.Dequeue()
		if result == nil {
			t.Fatal("Expected task, got nil")
		}
		if result.Priority != exp {
			t.Errorf("Expected priority %v, got %v", exp, result.Priority)
		}
	}
}
