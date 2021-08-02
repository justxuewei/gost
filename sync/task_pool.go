/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gxsync

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

import (
	gxruntime "github.com/dubbogo/gost/runtime"
)

type task func()

// GenericTaskPool represents an generic task pool.
type GenericTaskPool interface {
	// AddTask wait idle worker add task
	AddTask(t task) bool
	// AddTaskAlways add task to queues or do it immediately
	AddTaskAlways(t task)
	// AddTaskBalance add task to idle queue
	AddTaskBalance(t task)
	// Close uses to close the task pool
	Close()
	// IsClosed uses to check pool status.
	IsClosed() bool
}

func goSafely(fn func()) {
	gxruntime.GoSafely(nil, false, fn, nil)
}

/////////////////////////////////////////
// Task Pool
/////////////////////////////////////////

type TaskPool struct {
	TaskPoolOptions

	idx    uint32 // round robin index
	qArray []chan task
	wg     sync.WaitGroup

	once sync.Once
	done chan struct{}
}

// NewTaskPool build a task pool
func NewTaskPool(opts ...TaskPoolOption) GenericTaskPool {
	var tOpts TaskPoolOptions
	for _, opt := range opts {
		opt(&tOpts)
	}

	tOpts.validate()

	p := &TaskPool{
		TaskPoolOptions: tOpts,
		qArray:          make([]chan task, tOpts.tQNumber),
		done:            make(chan struct{}),
	}

	for i := 0; i < p.tQNumber; i++ {
		p.qArray[i] = make(chan task, p.tQLen)
	}
	p.start()

	return p
}

// start dispatches workers to different queues based on workerID
func (p *TaskPool) start() {
	for i := 0; i < p.tQPoolSize; i++ {
		p.wg.Add(1)
		workerID := i
		q := p.qArray[workerID%p.tQNumber]
		p.safeRun(workerID, q)
	}
}

// safeRun initializes a worker with error handler
func (p *TaskPool) safeRun(workerID int, q chan task) {
	gxruntime.GoSafely(nil, false,
		func() {
			err := p.run(workerID, q)
			if err != nil {
				// log error to stderr
				log.Printf("gost/TaskPool.run error: %s", err.Error())
			}
		},
		nil,
	)
}

// run assigns a task received from queue to the current worker
func (p *TaskPool) run(id int, q chan task) error {
	defer p.wg.Done()

	var (
		ok bool
		t  task
	)

	for {
		select {
		// pool was closed
		case <-p.done:
			if 0 < len(q) {
				return fmt.Errorf("task worker %d exit now while its task buffer length %d is greater than 0",
					id, len(q))
			}

			return nil
		// perform task received from queue
		case t, ok = <-q:
			if ok {
				func() {
					defer func() {
						if r := recover(); r != nil {
							_, _ = fmt.Fprintf(os.Stderr, "%s goroutine panic: %v\n%s\n",
								time.Now(), r, string(debug.Stack()))
						}
					}()
					t()
				}()
			}
		}
	}
}

// AddTask adds task to a queue randomly.
// Goroutine will be blocked if the queue is full.
// It returns false when the pool is stopped.
func (p *TaskPool) AddTask(t task) (ok bool) {
	idx := atomic.AddUint32(&p.idx, 1)
	id := idx % uint32(p.tQNumber)

	select {
	case <-p.done:
		return false
	default:
		p.qArray[id] <- t
		return true
	}
}

// AddTaskAlways adds task to a queue randomly.
// Goroutine will not be blocked if the queue is full. A new worker will be initialized and deal with the task
// immediately.
func (p *TaskPool) AddTaskAlways(t task) {
	id := atomic.AddUint32(&p.idx, 1) % uint32(p.tQNumber)

	select {
	case p.qArray[id] <- t:
		return
	default:
		goSafely(t)
	}
}

// AddTaskBalance adds task to an idle queue.
// Goroutine will not be blocked if most of the queues are full. A new worker will be initialized and deal with the task
// immediately.
func (p *TaskPool) AddTaskBalance(t task) {
	length := len(p.qArray)

	// try len/2 times to lookup idle queue
	for i := 0; i < length/2; i++ {
		select {
		case p.qArray[rand.Intn(length)] <- t:
			return
		default:
			continue
		}
	}

	goSafely(t)
}

func (p *TaskPool) stop() {
	select {
	case <-p.done:
		return
	default:
		p.once.Do(func() {
			close(p.done)
		})
	}
}

func (p *TaskPool) IsClosed() bool {
	select {
	case <-p.done:
		return true

	default:
		return false
	}
}

func (p *TaskPool) Close() {
	p.stop()
	p.wg.Wait()
	for i := range p.qArray {
		close(p.qArray[i])
	}
}

/////////////////////////////////////////
// Task Pool Simple
/////////////////////////////////////////
type taskPoolSimple struct {
	work chan task     // task channel
	sem  chan struct{} // gr pool size

	wg sync.WaitGroup

	once sync.Once
	done chan struct{}
}

// NewTaskPoolSimple build a simple task pool
func NewTaskPoolSimple(size int) GenericTaskPool {
	if size < 1 {
		size = runtime.GOMAXPROCS(-1) * 100
	}
	return &taskPoolSimple{
		work: make(chan task),
		sem:  make(chan struct{}, size),
		done: make(chan struct{}),
	}
}

func (p *taskPoolSimple) AddTask(t task) bool {
	select {
	case <-p.done:
		return false
	default:
	}

	select {
	case <-p.done:
		return false
	case p.work <- t:
	case p.sem <- struct{}{}:
		p.wg.Add(1)
		go p.worker(t)
	}
	return true
}

func (p *taskPoolSimple) AddTaskAlways(t task) {
	select {
	case <-p.done:
		return
	default:
	}

	select {
	case p.work <- t:
		// exec @t in gr pool
		return
	default:
	}
	select {
	case p.work <- t:
		// exec @t in gr pool
	case p.sem <- struct{}{}:
		// add a gr to the gr pool
		p.wg.Add(1)
		go p.worker(t)
	default:
		// gen a gr temporarily
		goSafely(t)
	}
}

func (p *taskPoolSimple) worker(t task) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "%s goroutine panic: %v\n%s\n",
				time.Now(), r, string(debug.Stack()))
		}
		p.wg.Done()
		<-p.sem
	}()
	t()
	for t := range p.work {
		t()
	}
}

// stop all tasks
func (p *taskPoolSimple) stop() {
	select {
	case <-p.done:
		return
	default:
		p.once.Do(func() {
			close(p.done)
			close(p.work)
		})
	}
}

func (p *taskPoolSimple) Close() {
	p.stop()
	// wait until all tasks done
	p.wg.Wait()
}

// check whether the session has been closed.
func (p *taskPoolSimple) IsClosed() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *taskPoolSimple) AddTaskBalance(t task) { p.AddTaskAlways(t) }
