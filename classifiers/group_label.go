package classifiers

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type GroupLabelOptions struct{}

func (o *GroupLabelOptions) GetLabel() string {
	return ""
}

func (o *GroupLabelOptions) Validate() error {
	return nil
}

type GroupLabelClassifier struct {
	mu      sync.Mutex
	items   map[string]struct{}
	label   string
	size    uint16
	waitCh  chan struct{}
	timeout time.Duration
}

func NewGroupLabelClassifier(label string, size uint16, timeout time.Duration) *GroupLabelClassifier {
	return &GroupLabelClassifier{
		items:   make(map[string]struct{}),
		waitCh:  make(chan struct{}),
		label:   label,
		size:    size,
		timeout: timeout,
	}
}

func (c *GroupLabelClassifier) Assign(ctx context.Context, _ Options, tid string) error {
	tid = strings.ToLower(strings.TrimSpace(tid))

	if len(tid) == 0 {
		return fmt.Errorf("group-label-classifier: %w: empty tid", ErrValidationFailed)
	}

	c.mu.Lock()

	if c.items == nil {
		c.items = make(map[string]struct{})
	}

	if _, found := c.items[tid]; found {
		return fmt.Errorf("group-label-classifier: %w: already exists: %s", ErrAssignmentFailed, tid)
	}

	c.mu.Unlock()

	if len(c.items) == int(c.size) {
		select {
		case <-c.waitCh:
		case <-ctx.Done():
			return fmt.Errorf("group-label-classifier: %w", ctx.Err())
		case <-time.After(c.timeout):
			return fmt.Errorf("group-label-classifier: %w", ErrAssignmentTimeout)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[tid] = struct{}{}

	return nil
}

func (c *GroupLabelClassifier) Unassign(tid string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.items) == 0 {
		return
	}

	tid = strings.ToLower(strings.TrimSpace(tid))

	if _, found := c.items[tid]; found {
		delete(c.items, tid)

		if len(c.items) == (int(c.size) - 1) {
			select {
			case c.waitCh <- struct{}{}:
			default:
			}
		}
	}
}

func (c *GroupLabelClassifier) Get(labels ...string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, label := range labels {
		if label == c.label {
			tids := make([]string, 0, len(c.items))

			for tid := range c.items {
				tids = append(tids, tid)
			}

			return tids
		}
	}

	return nil
}

func (c *GroupLabelClassifier) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.items)
}
