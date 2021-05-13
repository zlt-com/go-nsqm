package nsqm

import (
	"fmt"
)

// Task 定义任务接口,所有实现该接口的均实现工作池
type Task interface {
	DoTask() error
}

// Job 定义工作结构体
type Job struct {
	Task Task
}

// JobQueue 定义全部的工作队列
var JobQueue chan Job

// Worker 定义工作者
type Worker struct {
	WorkerPool chan chan Job // 工人对象池
	JobChannel chan Job      // 管道里面拿Job
	quit       chan bool
}

// NewWorker 新建一个工作者
func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,     // 工人对象池
		JobChannel: make(chan Job), //工人的任务
		quit:       make(chan bool),
	}
}

// StartWork 工作池启动主函数
func (w *Worker) StartWork() {
	// 开一个新的协程
	go func() {
		for {
			// 注册任务到工作池
			w.WorkerPool <- w.JobChannel
			select {
			// 接收到任务
			case job := <-w.JobChannel:
				// 执行任务
				err := job.Task.DoTask()
				if err != nil {
					fmt.Println("任务执行失败")
				}
				// time.Sleep(time.Second * 10)
			// 接收退出的任务, 停止任务
			case <-w.quit:
				return
			}
		}
	}()
}

// Stop 退出执行工作
func (w *Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

// Sender 定义任务发送者
type Sender struct {
	maxWorkers int           // 最大工人数
	WorkerPool chan chan Job // 注册工作通道
	quit       chan bool     // 退出信号
}

// NewSender 注册新发送者
func NewSender(maxWorkers int) *Sender {
	Pool := make(chan chan Job, maxWorkers)
	return &Sender{
		WorkerPool: Pool,       // 将工作者放到一个工作池中
		maxWorkers: maxWorkers, // 最大工作者数量
		quit:       make(chan bool),
	}
}

// Run 工作分发器
func (s *Sender) Run() {
	for i := 0; i < s.maxWorkers; i++ {
		worker := NewWorker(s.WorkerPool)
		// 执行任务
		worker.StartWork()
	}
	// 监控任务发送
	go s.Send()
}

// Quit 退出发放工作
func (s *Sender) Quit() {
	go func() {
		s.quit <- true
	}()
}

// Send 发送任务
func (s *Sender) Send() {
	for {
		select {
		// 接收到任务
		case job := <-JobQueue:
			go func(job Job) {
				jobChan := <-s.WorkerPool
				jobChan <- job
			}(job)
		// 退出任务分发
		case <-s.quit:
			return
		}
	}
}

// InitPool 初始化对象池
func InitPool(maxWorkers, maxQueue int) {
	// maxWorkers := 4
	// maxQueue := 1
	// 初始化一个任务发送者,指定工作者数量
	send := NewSender(maxWorkers)
	// 指定任务的队列长度
	JobQueue = make(chan Job, maxQueue)
	// fmt.Println("JobQueue:", JobQueue)
	// 一直运行任务发送
	send.Run()
}
