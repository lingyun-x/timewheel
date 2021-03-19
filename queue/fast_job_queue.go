/*
* Created by lingfeng on 2021/3/17 .
* Copyright (c) 2021 The LingYun Authors. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package queue

import (
	"github.com/lingyun-x/timewheel/job"
	"sync"
)

type FastReaderQueue struct {
	items []job.Job

	sync.Mutex
	noemptyCond *sync.Cond
	Disposed    bool
}

func NewFastReaderQueue() *FastReaderQueue {
	return &FastReaderQueue{
		noemptyCond: sync.NewCond(&sync.Mutex{}),
	}
}

func (q *FastReaderQueue) Get(index int) (interface{}, error) {
	for {
		itemLen := len(q.items)
		if itemLen > index {
			return q.items[index], nil
		}

		q.Mutex.Lock()
		itemLen = len(q.items)
		if itemLen > index {
			q.Mutex.Unlock()
			return q.items[index], nil
		}

		if q.Disposed {
			q.Mutex.Unlock()
			return nil, ErrDispose
		}

		q.noemptyCond.L.Lock()
		q.noemptyCond.Wait()
		q.noemptyCond.L.Unlock()
		q.Mutex.Unlock()
	}
}

func (q *FastReaderQueue) GetCount(index int, number int) ([]job.Job, error) {
	for {
		itemLen := len(q.items)
		if itemLen > index {
			endIndex := index + number
			if endIndex >= itemLen {
				endIndex = itemLen
			}
			return q.items[index:endIndex], nil
		}
		q.Mutex.Lock()
		itemLen = len(q.items)
		if itemLen > index {
			q.Mutex.Unlock()
			endIndex := index + number
			if endIndex >= itemLen {
				endIndex = itemLen
			}
			return q.items[index:endIndex], nil
		}

		if q.Disposed {
			q.Mutex.Unlock()
			return nil, ErrDispose
		}

		q.Mutex.Unlock()
		q.noemptyCond.L.Lock()
		q.noemptyCond.Wait()
		q.noemptyCond.L.Unlock()

	}
}

func (q *FastReaderQueue) GetAll(index int) ([]job.Job, error) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	itemLen := len(q.items)
	if itemLen > index {
		return q.items[index:], nil
	}
	if q.Disposed {
		return nil, ErrDispose
	}
	return nil, ErrQueueEmpty
}

func (q *FastReaderQueue) Put(values ...job.Job) error {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.items = append(q.items, values...)
	q.noemptyCond.Broadcast()
	return nil
}

func (q *FastReaderQueue) Len() int {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	return len(q.items)
}

func (q *FastReaderQueue) Dispose() {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Disposed = true
	q.noemptyCond.Broadcast()
}
