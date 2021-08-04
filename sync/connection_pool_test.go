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
	"github.com/stretchr/testify/assert"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionPool(t *testing.T) {
	t.Run("Count", func(t *testing.T) {
		p := NewConnectionPool(100, 100, nil)
		var count int64
		wg := new(sync.WaitGroup)
		for i := 1; i <= 100; i++ {
			wg.Add(1)
			value := i
			err := p.Submit(func() {
				defer wg.Done()
				atomic.AddInt64(&count, int64(value))
			})
			assert.Nil(t, err)
		}
		wg.Wait()
		assert.Equal(t, int64(5050), count)

		p.Close()
	})

	t.Run("PoolBusyErr", func(t *testing.T) {
		p := NewConnectionPool(1, 1, nil)
		_ = p.Submit(func() {
			time.Sleep(1 * time.Second)
		})

		err := p.Submit(func() {})
		assert.Equal(t, err, PoolBusyErr)

		time.Sleep(1 * time.Second)
		err = p.Submit(func() {})
		assert.Nil(t, err)

		p.Close()
	})
}

func BenchmarkConnectionPool(b *testing.B) {
	p := NewConnectionPool(runtime.NumCPU(), 1000000000, nil)

	b.Run("CountTask", func(b *testing.B) {
		task, _ := newCountTask()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := p.Submit(task); err != nil {
					panic("pool is full.")
				}
			}
		})
	})

	b.Run("CPUTask", func(b *testing.B) {
		task, _ := newCPUTask()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := p.Submit(task); err != nil {
					panic("pool is full.")
				}
			}
		})
	})

	b.Run("IOTask", func(b *testing.B) {
		task, _ := newIOTask()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := p.Submit(task); err != nil {
					panic("pool is full.")
				}
			}
		})
	})

	b.Run("RandomTask", func(b *testing.B) {
		task, _ := newRandomTask()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := p.Submit(task); err != nil {
					panic("pool is full.")
				}
			}
		})
	})

	p.Close()
}