# basic 示例

演示 pipeline 组件库的 **NextStage 链式连接**：每个 Stage 通过 `NextStage` 方法
创建并连接子 Stage，输入输出类型由编译器逐级强制匹配（`string → int → string`）。

## 运行

```bash
cd examples/basic
go run .
```

## 数据流

```
MockSource[string]               ["10", "20", "bad", "30"]（数字字符串）
        │
        ▼
stage-validate  string → int     strconv.Atoi 过滤非数字（"bad" 被拒绝）
        │
        ▼
stage-to-octal  int → string     strconv.FormatInt(v, 8)（10 → "12"）
        │
        ▼
stage-print     string → string  打印输出 8 进制
```

1. 定义根 Stage `s1`（`string → int`）；
2. `s1.NextStage("s2", cfg, fn)` 创建子 Stage（`int → string`），自动连线 `s2.input = s1.output`；
3. `s2.NextStage("s3", cfg, fn)` 创建打印 Stage（`string → string`）；
4. `New[string, int](cfg).AddStage(s1)` 将 `s1` 设为 Pipeline 根 Stage，`T1=string`（输入类型）、`T2=int`（根 Stage 输入类型）；
5. `MockSource` 注入 `10 / 20 / bad / 30`；
6. Start/Close 递归传播：`s1.Start` 先启动子 Stage，再启动自身；`s1.Close` 先关闭自身，再关闭子 Stage。

## 预期输出

```
octal: 12      （十进制 10 的 8 进制表示）
octal: 24      （十进制 20 的 8 进制表示）
octal: 36      （十进制 30 的 8 进制表示）
Pipeline 正常退出
```