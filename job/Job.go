package job

import "time"

type Job interface {
	ExecuteTime() time.Time
	Run()
}
