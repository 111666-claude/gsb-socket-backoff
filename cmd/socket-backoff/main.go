// Command socket-backoff 跑重连调度样例。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"example.com/backoff"
)

// Run 执行一次命令行调用，返回退出码。
func Run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("socket-backoff", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sample := flags.String("sample", "retry-after", "retry-after / budget / cap / reset")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *sample == "retry-after" {
		scheduler := backoff.NewScheduler()
		delay, _ := scheduler.Next("c1", 0, 60000)
		fmt.Fprintf(stdout, "delay=%d\n", delay)
		return 0
	}
	if *sample == "budget" {
		scheduler := backoff.NewScheduler()
		allowed := 0
		for index := 0; index < 200; index++ {
			if _, ok := scheduler.Next(fmt.Sprintf("c-%d", index), 0, 0); ok {
				allowed++
			}
		}
		fmt.Fprintf(stdout, "allowed=%d\n", allowed)
		return 0
	}
	if *sample == "cap" {
		scheduler := backoff.NewScheduler()
		last := int64(0)
		for index := 0; index < 20; index++ {
			last, _ = scheduler.Next("c1", int64(index)*1000, 0)
		}
		fmt.Fprintf(stdout, "delay=%d\n", last)
		return 0
	}
	if *sample == "reset" {
		scheduler := backoff.NewScheduler()
		scheduler.Next("c1", 0, 0)
		scheduler.Next("c1", 1000, 0)
		scheduler.Next("c1", 2000, 0)
		scheduler.Success("c1")
		delay, _ := scheduler.Next("c1", 100000, 0)
		fmt.Fprintf(stdout, "in_range=%v\n", delay >= 375 && delay <= 625)
		return 0
	}
	fmt.Fprintln(stderr, "需要 --sample retry-after|budget|cap|reset")
	return 2
}

func main() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}
