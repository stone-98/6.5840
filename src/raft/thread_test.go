package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestThread(t *testing.T) {

	// 创建一个持续时间为2秒的计时器
	timer := time.NewTimer(2 * time.Second)

	fmt.Println("等待计时器触发...")

	go func(timer *time.Timer) {
		timer.Stop()
		//timer.Reset(5 * time.Second)
	}(timer)

	// 使用 <-timer.C 来等待计时器触发
	<-timer.C

	fmt.Println("计时器已触发！")
}
