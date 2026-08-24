package pipeline

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SignalContext 返回绑定常用中断信号（SIGINT / SIGTERM）的 Context（D-34）。
//
// 用途：作为 Pipeline.Run 的 ctx，进程收到 Ctrl+C 或 kill 时 ctx 自动取消，
// 触发级联优雅关闭——无需在每个调用方手写 signal.NotifyContext。
//
//	ctx, stop := pipeline.SignalContext()
//	defer stop()
//	if err := pl.Run(ctx); err != nil { ... }
//	pl.Close(5 * time.Second) // 信号触发后由调用方触发关闭
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}