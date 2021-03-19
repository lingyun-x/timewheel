/*
* Created by lingfeng on 2021/3/18 .
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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type timeJob struct {
	executeTime time.Time
	run         func()
}

func (j *timeJob) ExecuteTime() time.Time {
	return j.executeTime
}

func (j *timeJob) Run() {
	//log.Printf("execute job expectTime %v, actual:%v \n", j.executeTime, time.Now())
	delay := time.Now().Sub(j.executeTime)
	if delay > time.Second*1 || delay < -time.Second*1 {
		log.Printf("execute job finished expect time:%v actual:%v\n", j.executeTime, time.Now())
		log.Printf("execute job finished pass time:%v", delay)
	}
	j.run()

}

func Test1(*testing.T) {
	log.Printf("start %v\n", time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	w := NewTimeWheel(ctx, cancel, 1*time.Second, time.Now(), 10)
	w.Start()

	wg := sync.WaitGroup{}

	var doneJobs int32 = 0

	for i := 0; i < 100_000; i++ {
		time.Sleep(time.Duration(rand.Intn(50)+50) * time.Millisecond)
		for j := 0; j < 100; j++ {
			wg.Add(1)

			job := &timeJob{
				executeTime: time.Now().Add(time.Duration(rand.Intn(2*60)) * time.Second),
				run: func() {
					wg.Done()
					atomic.AddInt32(&doneJobs, 1)

					if doneJobs%1000 == 0 {
						log.Printf("doneJobs %d\n", doneJobs)
					}
				},
			}
			err := w.Execute(job)
			if err != nil {
				log.Printf("err:%v \n", err)
			}
		}

	}
	log.Printf("add job finish")
	wg.Wait()

	log.Println("------------------finish -----------------")
}
