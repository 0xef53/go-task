package task

import (
	"time"

	test_utils "github.com/0xef53/go-task/internal/testing"
)

const (
	modeChangePropertyName OperationMode = 1 << (16 - 1 - iota)
	modeChangePropertyDiskName
	modeChangePropertyDiskSize
	modeChangePropertyNetName
	modeChangePropertyNetLink
	modePowerUp
	modePowerDown
	modePowerCycle

	modeAny                = ^OperationMode(0)
	modeChangePropertyDisk = modeChangePropertyDiskName | modeChangePropertyDiskSize
	modeChangePropertyNet  = modeChangePropertyNetName | modeChangePropertyNetLink
	modeChangeProperties   = modeChangePropertyDisk | modeChangePropertyNet
	modePowerManagement    = modePowerUp | modePowerDown | modePowerCycle
)

type poolTest_dummyTask struct {
	*GenericTask

	targets map[string]OperationMode

	id       string
	lifetime time.Duration

	SleepBeforeStart        bool
	FailBeforeStartFunction bool
	FailOnSuccessFunction   bool
}

func (t *poolTest_dummyTask) Targets() map[string]OperationMode { return t.targets }

func (t *poolTest_dummyTask) BeforeStart(a interface{}) error {
	if t.SleepBeforeStart {
		time.Sleep(t.lifetime * time.Second)
	}

	if t.FailBeforeStartFunction {
		return test_utils.ErrSuccessfullyFailed
	}

	return nil
}

func (t *poolTest_dummyTask) OnSuccess() error {
	if t.FailOnSuccessFunction {
		return test_utils.ErrSuccessfullyFailed
	}

	return nil
}

func (t *poolTest_dummyTask) Main() error {
	if t.lifetime > 0 {
		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		case <-time.After(t.lifetime * time.Second):
		}
	}

	return nil
}
