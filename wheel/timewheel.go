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
package wheel

import (
	"context"
	"github.com/lingyun-x/timewheel/job"
	queue2 "github.com/lingyun-x/timewheel/queue"
	"log"
	"sync"
	"time"
)

type slots struct {
	context.Context
	ss            []*slot
	slotTimeUnit  time.Duration
	startTime     time.Time
	endTime       time.Time
	slotsDuration time.Duration
}

func (s *slots) timeElapse(slotPoint int) {
	for i := 0; i < slotPoint; i++ {
		//release slot
		s.ss[i] = nil
	}
}

func newSlots(ctx context.Context, slotTimeUnit time.Duration, slotCount int, startTime time.Time) *slots {
	slots := &slots{
		slotTimeUnit: slotTimeUnit,
		ss:           make([]*slot, slotCount),
	}

	slots.startTime = startTime
	slots.endTime = startTime.Add(time.Duration(slotCount) * slotTimeUnit)
	slots.slotsDuration = slots.endTime.Sub(slots.startTime)
	ctx2, _ := context.WithDeadline(ctx, slots.endTime)
	slots.Context = ctx2

	var endTime time.Time
	for i := 0; i < slotCount; i++ {
		endTime = startTime.Add(slotTimeUnit)
		s := newSlot(ctx2, slotTimeUnit, startTime, endTime)
		slots.ss[i] = s
		startTime = endTime
	}

	return slots
}

func (s *slots) getSlot(time2 time.Time) (*slot, error) {
	startTime, endTime := s.startTime, s.endTime

	if time2.Before(startTime) {
		return nil, nil
	}

	if time2.After(endTime) || time2.Equal(endTime) {
		return nil, nil
	}

	du := time2.Sub(startTime)
	index := du / s.slotTimeUnit

	return s.ss[index], nil
}

func (s *slots) dispatchJob(job2 job.Job) error {
	time2 := job2.ExecuteTime()
	si := time2.Sub(s.startTime) / s.slotTimeUnit
	err := s.ss[si].put(job2)
	return err
}

type slot struct {
	context.Context
	timeUnit  time.Duration
	startTime time.Time
	endTime   time.Time
	jobQueue  *queue2.FastReaderQueue
	jobIndex  int
}

func newSlot(ctx context.Context, timeUnit time.Duration, startTime time.Time, endTime time.Time) *slot {
	ctx2, _ := context.WithDeadline(ctx, endTime)
	return &slot{
		Context:   ctx2,
		jobQueue:  queue2.NewFastReaderQueue(),
		timeUnit:  timeUnit,
		startTime: startTime,
		endTime:   endTime,
	}
}

func (s *slot) put(jobs ...job.Job) error {
	return s.jobQueue.Put(jobs...)
}

func (s *slot) getJobs(count int) ([]job.Job, error) {
	jobs, err := s.jobQueue.GetCount(s.jobIndex, count)
	if err == nil && len(jobs) > 0 {
		s.jobIndex += len(jobs)
		return jobs, err
	}
	return nil, err
}

func (s slot) getAllJobs() ([]job.Job, error) {
	jobs, err := s.jobQueue.GetAll(s.jobIndex)
	if err == nil && len(jobs) > 0 {
		s.jobIndex += len(jobs)
	}
	return jobs, err
}

func (s *slot) close() {
	s.jobQueue.Dispose()
}

type TimeWheelGenerator = func(ctx context.Context, cancel context.CancelFunc, slotTimeUnit time.Duration, startTime time.Time) *TimeWheel

var DefaultTimeWheelGenerator = func(ctx context.Context, cancel context.CancelFunc, slotTimeUnit time.Duration, startTime time.Time) *TimeWheel {
	return NewTimeWheel(ctx, cancel, slotTimeUnit, startTime, 12)
}

type TimeWheel struct {
	context.Context
	Cancel       context.CancelFunc
	mutex        sync.Mutex
	wheelLength  int
	slots        *slots
	slotTimeUnit time.Duration

	slotPoint int
	//时间轮开始时间
	StartTime time.Time
	EndTime   time.Time
	//最后调度时间
	LatestScheduleTime time.Time
	paused             bool
	resumeCond         sync.Cond

	pre  *TimeWheel
	next *TimeWheel
}

func NewTimeWheel(ctx context.Context, cancel context.CancelFunc, slotTimeUnit time.Duration, startTime time.Time, wheelLength int) *TimeWheel {
	if slotTimeUnit < time.Second {
		slotTimeUnit = time.Second
	}

	endTime := startTime.Add(time.Duration(wheelLength) * slotTimeUnit)
	ss := newSlots(ctx, slotTimeUnit, wheelLength, startTime)
	w := &TimeWheel{
		Context:      ctx,
		Cancel:       cancel,
		wheelLength:  wheelLength,
		slotTimeUnit: slotTimeUnit,
		slots:        ss,
		StartTime:    startTime,
		EndTime:      endTime,
	}

	return w
}

func (w *TimeWheel) Start() {
	go func() {
		w.slotPoint = -1
		for {
			w.timeElapse(w.slotPoint + 1)
			//wait
			<-w.slots.ss[w.slotPoint].Context.Done()

			//if the time wheel is paused wait for resume
			if w.paused {
				w.mutex.Lock()

				if w.paused {
					w.mutex.Unlock()

					w.resumeCond.L.Lock()
					w.resumeCond.Wait()
					w.resumeCond.L.Unlock()
					continue
				}
				w.mutex.Unlock()
			}
		}
	}()
}

func (w *TimeWheel) overFlowTimeWheel() {
	//over flow
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.slotPoint = 0
	ss := newSlots(w.Context, w.slotTimeUnit, len(w.slots.ss), w.slots.endTime)
	w.slots = ss
	w.StartTime = ss.startTime
	w.EndTime = ss.endTime

	if w.next == nil {
		//to auto create next time wheel
		ctx, cancel := context.WithCancel(w.Context)
		nextTimeUnit := w.EndTime.Sub(w.StartTime)
		next := DefaultTimeWheelGenerator(ctx, cancel, nextTimeUnit, w.StartTime)
		w.next = next
		next.pre = w
	}

	w.next.timeElapse(w.next.slotPoint + 1)
}

//move time slot to special
func (w *TimeWheel) timeElapse(slotPoint int) {
	if slotPoint >= w.wheelLength {
		w.overFlowTimeWheel()
	} else {
		w.mutex.Lock()
		w.slotPoint = slotPoint
		w.mutex.Unlock()

		w.slots.timeElapse(slotPoint)
	}

	//if w.pre is not nil dispatch current slot jobs to w.pre
	if w.pre != nil {
		w.dispatchCurrentJobsToPre()
	} else {
		//else to execute current slot jobs
		slot := w.slots.ss[w.slotPoint]

		go func() {
			<-slot.Context.Done()
			slot.close()
		}()

		for {
			jobs, err := slot.getJobs(200)
			if err != nil {
				log.Printf("slot.getJobs err:%v", err)
				break
			}

			if len(jobs) > 0 {
				w.doJobs(jobs)
			}
		}

	}
}

func (w *TimeWheel) doJobs(jobs []job.Job) {
	for _, j := range jobs {
		go func() {
			defer func() {
				err := recover()
				if err != nil {
					log.Printf("slot job recover err:%v", err)
				}
			}()
			j.Run()
		}()
	}

}

func (w *TimeWheel) dispatchCurrentJobsToPre() {
	slot := w.slots.ss[w.slotPoint]
	jobs, err := slot.getAllJobs()

	if err != nil {
		log.Printf("getALl jobs err:%v", err)
	}

	if jobs != nil {
		for _, j := range jobs {
			err = w.pre.dispatchJob(j)
			if err != nil {
				log.Printf("dispatchJob err:%v", err)
			}
		}
	}
}

//dispatch job to time wheel
func (w *TimeWheel) dispatchJob(job2 job.Job) error {
	time2 := job2.ExecuteTime()
	startTime := w.slots.ss[w.slotPoint].startTime

	if time2.Before(startTime) {
		return ErrTimeExpire
	}

	if time2.After(w.EndTime) {
		if w.next == nil {
			//to auto create next time wheel
			ctx, cancel := context.WithCancel(w.Context)
			nextTimeUnit := w.EndTime.Sub(w.StartTime)
			next := DefaultTimeWheelGenerator(ctx, cancel, nextTimeUnit, w.StartTime)
			w.next = next
			next.pre = w
		}

		return w.next.dispatchJob(job2)
	}

	return w.slots.dispatchJob(job2)
}

func (w *TimeWheel) Pause() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.paused = true
}

func (w *TimeWheel) Resume() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.paused = false
	w.resumeCond.Broadcast()
}

func (w *TimeWheel) Execute(job2 job.Job) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.dispatchJob(job2)
}

func (w *TimeWheel) ExecuteJobs(jobs ...job.Job) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for _, j := range jobs {
		err := w.dispatchJob(j)
		if err != nil {
			log.Fatal(err)
		}
	}
	return nil
}

func (w *TimeWheel) Deeps() int {
	deeps := 1

	for next := w.next; next != nil; next = next.next {
		deeps++
	}
	return deeps
}

func (w *TimeWheel) GetDeepWheel(deep int) *TimeWheel {
	tw := w
	for i := 0; i < deep; i++ {
		tw = tw.next
		if tw == nil {
			return nil
		}
	}
	return tw
}
