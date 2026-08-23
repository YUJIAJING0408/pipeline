// Command basic 演示 pipeline 组件库的最小用法（同构链，装箱前形态）。
//
// 场景：MockSource 注入数字字符串 → 3 个 Stage 串联处理：
// stage1 过滤非数字输入 → stage2 十进制转 8 进制字符串 → stage3 打印输出。
// 整条链载荷类型统一为 string（Pipeline[T] 同构约束）。
//
// 运行：
//
//	cd examples/basic && go run .
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/example/pipeline"
)

func main() {
	// 1. 定义 3 个同类型（string → string）Stage。
	//    stage1: 过滤非数字字符串（如 mock 注入的坏数据）
	//    首节点 input 传 nil：注册 InputSource 时由 Run 创建注入通道接管。
	s1 := pipeline.NewStage("stage-validate", pipeline.StageConfig{
		Workers: 2, OutCap: 16, Timeout: 5 * time.Second,
	}, nil, nil, func(ctx context.Context, in string) (int, error) {
		out, err := strconv.Atoi(in)
		if err != nil {
			return 0, fmt.Errorf("非数字字符串: %q", in)
		}
		return out, nil
	})

	//    stage2: 十进制字符串转 8 进制字符串（如 "10" → "12"）
	//    后续节点 input 传 nil：AddStage 连线时指向前驱 output。
	s2 := s1.NextStage("stage-to-octal", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, in int) (string, error) {
		return strconv.FormatInt(int64(in), 8), nil
	})

	//    stage3: 打印输出（sink）
	s2.NextStage("stage-print", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, in string) (struct{}, error) {
		fmt.Println("octal:", in)
		return struct{}{}, nil
	})

	// 2. 组装 Pipeline：配置（D-18）+ 同构链 + Mock 输入源。
	pl := pipeline.New[string, int](pipeline.PipelineConfig{
		Name:            "basic-example",
		InputBufferSize: 16,
		LogDir:          "./logs",
		LogEnabled:      true,
	}).Input(&pipeline.MockSource[string]{Data: []string{"10", "20", "bad", "30"}}).AddStage(s1)

	pl.Describe()
	println(pl.GraphTD())

	// 3. 运行（阻塞），信号触发级联优雅关闭。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		if err := pl.Close(5 * time.Second); err != nil {
			fmt.Fprintln(os.Stderr, "close error:", err)
		}
	}()

	if err := pl.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "run error:", err)
	}
	fmt.Println("Pipeline 正常退出")
}
