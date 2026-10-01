// Command socket-backoff 跑重连退避样例。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"example.com/backoff"
)

// Run 执行一次命令行调用，返回退出码。
func Run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("socket-backoff", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sample := flags.String("sample", "growth", "growth / cap / reset / work")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	switch *sample {
	case "growth":
		scheduler := backoff.NewScheduler()
		values := make([]string, 0, 4)
		for index := 0; index < 4; index++ {
			values = append(values, fmt.Sprint(scheduler.Next("c1")))
		}
		fmt.Fprintf(stdout, "delays=%s\n", strings.Join(values, ","))
	case "cap":
		scheduler := backoff.NewScheduler()
		last := 0
		for index := 0; index < 20; index++ {
			last = scheduler.Next("c1")
		}
		fmt.Fprintf(stdout, "delay=%d\n", last)
	case "reset":
		scheduler := backoff.NewScheduler()
		scheduler.Next("c1")
		scheduler.Next("c1")
		scheduler.Next("c1")
		scheduler.Success("c1")
		fmt.Fprintf(stdout, "delay=%d\n", scheduler.Next("c1"))
	case "work":
		scheduler := backoff.NewScheduler()
		for index := 0; index < 1000; index++ {
			scheduler.Next("c1")
		}
		fmt.Fprintf(stdout, "work=%d\n", scheduler.Work())
	default:
		fmt.Fprintln(stderr, "需要 --sample growth|cap|reset|work")
		return 2
	}
	return 0
}

func main() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}
