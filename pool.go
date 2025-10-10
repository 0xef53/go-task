package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var (
	ErrPoolClosed = errors.New("pool is closed")
)

// TaskOption represents a generic option that can be passed when starting a new task.
type TaskOption interface{}

// Pool manages a collection of concurrent tasks, providing thread-safe operations on them.
type Pool struct {
	mu    sync.Mutex
	table map[string]Task

	reporter Reporter

	classifier *rootClassifier

	wg       sync.WaitGroup
	isClosed bool

	unread map[string]*TaskStat
}

// NewPool returns a new instance of a task pool.
func NewPool() *Pool {
	return &Pool{
		table:      make(map[string]Task),
		classifier: newRootClassifier(),
		unread:     make(map[string]*TaskStat),
	}
}

// SetReporter sets the given [Reporter] instance as the main reporter.
// This reporter will receive task status and progress updates.
func (p *Pool) SetReporter(r Reporter) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.reporter = r
}

// sendReport sends the current status of the given task to the main reporter (if it set).
func (p *Pool) sendReport(ctx context.Context, t Task) {
	if p.reporter == nil {
		return
	}

	p.reporter.Send(ctx, t.Stat())
}

// RegisterClassifier registers a new [TaskClassifier] under one or more names.
// If multiple names are specified, the first one will be the primary one,
// and the rest will be aliases.
// If no names are provided, a default name is generated based on the classifier's type.
//
// Returns the registered names or an error if registration fails.
func (p *Pool) RegisterClassifier(c TaskClassifier, names ...string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.classifier.Register(c, names...)
}

// StartTask starts the provided task asynchronously with optional configuration options.
//
// Before start:
//   - if the task contains target locks, it will be checked for conflicts with
//     currently running tasks.
//   - if classifiers are specified in options, they will be attached to the task and
//     may affect whether the task can start immediately, for example by limiting the number
//     of concurrent tasks in a group.
//
// The optional parameter resp can be used to return data from the BeforeStart function.
//
// Returns the task ID or an error if starting fails.
//
// Example:
//
//	pool := task.NewPool()
//
//	requisites := Requisites{}
//
//	t := IncomingMigrationTask{
//		GenericTask: new(task.GenericTask),
//		vmname: vmname,
//	}
//
//	taskOpts := []task.TaskOption{
//		&task.TaskClassifierDefinition{
//			Name: "unique-labels",
//			Opts: &classifiers.UniqueLabelOptions{Label: vmname+"/migration"},
//		},
//		&task.TaskClassifierDefinition{
//			Name: "group-migrations",
//			Opts: &classifiers.LimitedGroupOptions{},
//		},
//	}
//
//	ctx = context.WithoutCancel(ctx)
//
//	md := reporter.Metadata{
//		DisplayName: fmt.Sprintf("%T", t),
//	}
//
//	ctx = task_metadata.AppendToContext(ctx, &md)
//
//	_, err := s.TaskStart(ctx, &t, &requisites)
//	if err != nil {
//		panic("cannot start incoming instance: " + err.Error())
//	}
func (p *Pool) StartTask(ctx context.Context, t Task, resp interface{}, opts ...TaskOption) (string, error) {
	err := func() error {
		if p.isClosed {
			return ErrPoolClosed
		}

		var success bool

		p.wg.Add(1)
		defer func() {
			if !success {
				p.wg.Done()
			}
		}()

		// The low level embedded task interface
		eti, ok := t.(interface {
			init(context.Context, string, chan<- int)
			release(error)
		})
		if !ok {
			return fmt.Errorf("invalid embedded interface")
		}

		// New task ID
		tid := uuid.New().String()

		// Parse task options
		for _, opt := range opts {
			switch o := opt.(type) {
			case *TaskClassifierDefinition:
				if err := p.classifier.Assign(ctx, o, tid); err != nil {
					return err
				}
			}
		}
		defer func() {
			if !success {
				p.classifier.Unassign(tid)
			}
		}()

		// Verify if the context was closed in the previous step
		if err := ctx.Err(); err != nil {
			return err
		}

		p.mu.Lock()

		delete(p.unread, tid)

		// Get all running tasks and check if a new task conflicts with them
		for tid := range p.table {
			if targets := p.table[tid].Targets(); p.table[tid].IsRunning() && len(targets) > 0 {
				for object, newActions := range t.Targets() {
					if _, ok := targets[object]; ok && targets[object]&newActions != 0 {
						p.mu.Unlock()

						return &ConcurrentRunningError{fmt.Sprintf("%T", t), targets}
					}
				}
			}
		}

		// Will be closed in the task release() function
		progressCh := make(chan int, 8)

		// Initialize ...
		eti.init(ctx, tid, progressCh)

		p.table[t.ID()] = t

		logger := log.WithFields(log.Fields{"task-id": t.ShortID()})

		p.mu.Unlock()

		p.sendReport(ctx, t)

		// Progress reporter
		if p.reporter != nil {
			go p.reporter.SendProgress(ctx, t.ID(), progressCh)
		}

		// ... and run the pre-start hook
		if err := t.BeforeStart(resp); err != nil {
			logger.Errorf("Pre-start function failed: %s", err)

			eti.release(err)

			p.mu.Lock()
			delete(p.table, t.ID())
			p.mu.Unlock()

			return err
		}

		success = true

		// Main background process
		go func() {
			var err error

			defer func() {
				eti.release(err)

				p.classifier.Unassign(t.ID())

				p.wg.Done()

				p.sendReport(ctx, t)

				go func() {
					time.Sleep(30 * time.Second)

					p.mu.Lock()
					defer p.mu.Unlock()

					if t, found := p.table[t.ID()]; found && !t.IsRunning() {
						p.unread[t.ID()] = t.Stat()

						delete(p.table, t.ID())
					}
				}()
			}()

			err = t.Main()

			if err == nil {
				logger.Info("Successfully completed")

				err = t.OnSuccess()
			} else {
				logger.Errorf("Fatal error: %s", err)

				t.OnFailure(err)
			}
		}()

		return nil
	}()

	if err != nil {
		return "", err
	}

	return t.ID(), nil
}

// Stat returns a slice of [TaskStat] structs representing the statistics
// (ID, progress, state, any error information) of tasks identified by the given keys.
// The keys can be specific task IDs or sets of labels that may correspond to multiple task IDs
// (e.g., group classifiers).
// If no keys are provided, it returns statistics for all tasks currently in the pool.
func (p *Pool) Stat(keys ...string) []*TaskStat {
	p.mu.Lock()
	defer p.mu.Unlock()

	m := make(map[string]*TaskStat)

	// Run through the summary list of task IDs satisfying the given keys
	for _, tid := range p.ids(keys...) {
		if t, found := p.table[tid]; found {
			m[tid] = t.Stat()
		} else {
			if st, found := p.unread[tid]; found {
				delete(p.unread, tid)

				m[tid] = st
			}
		}
	}

	stats := make([]*TaskStat, 0, len(m))

	for _, st := range m {
		if st != nil {
			stats = append(stats, st)
		}
	}

	return stats
}

// Metadata returns a slice of user-defined metadata for the tasks identified by the given keys.
// The keys can be specific task IDs or sets of labels that may correspond to multiple task IDs
// (e.g., group classifiers).
// If no keys are provided, the function returns an empty slice.
func (p *Pool) Metadata(keys ...string) []interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(keys) == 0 {
		return make([]interface{}, 0)
	}

	m := make(map[string]interface{})

	// Run through the summary list of task IDs satisfying the given keys
	for _, tid := range p.ids(keys...) {
		if t, found := p.table[tid]; found {
			m[tid] = t.Metadata()
		}
	}

	data := make([]interface{}, 0, len(m))

	for _, md := range m {
		if md != nil {
			data = append(data, md)
		}
	}

	return data
}

// Err returns the error associated with the task identified by tid, if any.
// Returns nil if the task is not found or has no error.
func (p *Pool) Err(tid string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if t, found := p.table[tid]; found {
		return t.Err()
	}

	return nil
}

// Cancel cancels the tasks identified by the given keys.
// The keys can be specific task IDs or sets of labels that may correspond to multiple task IDs
// (e.g., group classifiers).
// If no keys are provided, no tasks are cancelled.
func (p *Pool) Cancel(keys ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(keys) > 0 {
		// Run through the summary list of task IDs satisfying the given keys
		for _, tid := range p.ids(keys...) {
			if t, found := p.table[tid]; found {
				t.Cancel()
			}
		}
	}
}

// Wait blocks until the task identified by tid is released (completed or cancelled or failed),
// if it exists in the pool.
func (p *Pool) Wait(tid string) {
	t := func() Task {
		p.mu.Lock()
		defer p.mu.Unlock()

		if t, found := p.table[tid]; found {
			return t
		}

		return nil
	}()

	if t != nil {
		t.Wait()
	}
}

// List returns a slice of task IDs from the pool, that match the provided keys.
// The keys can be specific task IDs or sets of labels that may correspond to multiple task IDs
// (e.g., group classifiers).
// If no keys are given, the function returns IDs of all tasks currently in the pool.
func (p *Pool) List(keys ...string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ids(keys...)
}

// WaitAndClosePool waits for all running tasks to complete and marks the pool as closed,
// preventing new tasks from being started.
func (p *Pool) WaitAndClosePool() {
	p.wg.Wait()

	p.isClosed = true
}

// RunFunc creates and starts a function-based task with the specified target and options.
// If wait is true, it blocks until the task completes.
//
// Returns the task ID and any error encountered during execution.
//
// Example:
//
//	pool := task.NewPool()
//
//	taskOpts := []task.TaskOption{
//		// ...
//	}
//
//	blockUntilCompleted := true
//
//	err := pool.TaskRunFunc(ctx, tgt, blockUntilCompleted, taskOpts, func(l *log.Entry) error {
//		return doSomething()
//	})
func (p *Pool) RunFunc(ctx context.Context, tgt map[string]OperationMode, wait bool, opts []TaskOption, fn func(*log.Entry) error) (string, error) {
	task := FuncTask{new(GenericTask), tgt, fn}

	tid, err := p.StartTask(ctx, &task, nil, opts...)
	if err != nil {
		return "", err
	}

	if wait {
		task.Wait()
	}

	return tid, task.Err()
}

func (p *Pool) ids(keys ...string) []string {
	if len(keys) == 0 {
		result := make([]string, 0, len(p.table))

		for tid := range p.table {
			result = append(result, tid)
		}

		return result
	}

	m := make(map[string]struct{})

	// Get task IDs from classifiers using the given keys as labels
	for _, tid := range p.classifier.Get(keys...) {
		if _, found := p.table[tid]; found {
			m[tid] = struct{}{}
		}
	}

	// Сheck if there are task IDs in the given keys
	for _, tid := range keys {
		if _, found := p.table[tid]; found {
			m[tid] = struct{}{}
		}
	}

	result := make([]string, 0, len(m))

	for tid := range m {
		result = append(result, tid)
	}

	return result
}
