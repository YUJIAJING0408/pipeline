package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkSingleStage 测量单 Stage 的吞吐量。
func BenchmarkSingleStage(b *testing.B) {
	cap := b.N
	if cap < 1024 {
		cap = 1024
	}
	in := make(chan int, cap)
	out := make(chan int, cap)
	s := NewStage("bench", StageConfig{Workers: 4, OutCap: cap}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	out = s.output
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Start(ctx, nil); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in <- i
		<-out
	}
	b.StopTimer()
	cancel()
	close(in)
	s.pool.wait()
}

// BenchmarkChain3 测量 3 级链式 Stage 的吞吐量。
func BenchmarkChain3(b *testing.B) {
	cap := b.N
	if cap < 1024 {
		cap = 1024
	}
	in := make(chan int, cap)
	s1 := NewStage("s1", StageConfig{Workers: 4, OutCap: cap}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	s3 := s1.NextStage("s2", StageConfig{Workers: 4, OutCap: cap}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	}).NextStage("s3", StageConfig{Workers: 4, OutCap: cap}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	if err := s1.Start(ctx, nil); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in <- i
		<-s3.output
	}
	close(in)
	time.Sleep(50 * time.Millisecond)
	b.StopTimer()
	cancel()
	s1.pool.wait()
}

// BenchmarkWorkerCount 比较不同 Worker 数下的吞吐量。
func BenchmarkWorkerCount(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			cap := b.N
			if cap < 1024 {
				cap = 1024
			}
			in := make(chan int, cap)

			s := NewStage("bench", StageConfig{Workers: workers, OutCap: cap}, in, nil, func(ctx context.Context, x int) (int, error) {
				return x, nil
			})
			out := s.output
			ctx, cancel := context.WithCancel(context.Background())
			if err := s.Start(ctx, nil); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- i
				<-out
			}
			b.StopTimer()
			cancel()
			close(in)
			s.pool.wait()
		})
	}
}

// BenchmarkIOWorkerCount 模拟 IO 密集型处理（1µs 延迟），验证 Worker 扩展性。
func BenchmarkIOWorkerCount(b *testing.B) {
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			cap := 1024
			in := make(chan int, cap)

			s := NewStage("io", StageConfig{Workers: workers, OutCap: cap}, in, nil, func(ctx context.Context, x int) (int, error) {
				time.Sleep(50 * time.Microsecond)
				return x, nil
			})
			out := s.output
			ctx, cancel := context.WithCancel(context.Background())
			if err := s.Start(ctx, nil); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- i
				<-out
			}
			close(in)
			b.StopTimer()
			cancel()
			s.pool.wait()
		})
	}
}
