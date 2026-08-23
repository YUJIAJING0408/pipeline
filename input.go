package pipeline

import "context"

// InputSource 提供 Pipeline 的初始输入。用户在 Pipeline 上调用 Input() 注册。
//
// 契约：Start 向 out 持续发送数据，数据全部发完或 ctx 取消时返回（**不负责
// close**）；close 由 Pipeline.Run 在等待所有输入源返回后统一执行，避免
// 多源场景下某源先关闭导致其他源 panic。
type InputSource[In any] interface {
	// Start 启动注入逻辑，通过 out 发送数据；ctx 取消时停止。发送完毕自然返回。
	Start(ctx context.Context, out chan<- In) error
}

// MockSource 内置测试输入源：按顺序发送固定数据后自然返回。
type MockSource[In any] struct {
	// Data 为待发送的固定数据列表。
	Data []In
}

// Start 实现 InputSource：顺序发送 Data，ctx 取消时提前返回。
func (s *MockSource[In]) Start(ctx context.Context, out chan<- In) error {
	for _, item := range s.Data {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- item:
		}
	}
	return nil
}
