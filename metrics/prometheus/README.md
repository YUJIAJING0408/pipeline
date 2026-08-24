# Prometheus Exporter (pipeline 子模块)

将 Pipeline 运行时指标暴露为 Prometheus 格式，用于 Grafana 看板 / 告警。

## 使用方式

```bash
go get github.com/YUJIAJING0408/pipeline/metrics/prometheus
```

```go
import (
    pipeline "github.com/YUJIAJING0408/pipeline"
    promexporter "github.com/YUJIAJING0408/pipeline/metrics/prometheus"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

exporter := promexporter.New(pl.MetricsMonitor())
prometheus.MustRegister(exporter)
http.Handle("/metrics", promhttp.Handler())
```

## 运行

```bash
# 一键启动（Docker 编译 + Prometheus + Grafana 自动配置）
cd metrics/prometheus/example && docker-compose up --build

# 访问地址：
#   Pipeline 指标: http://localhost:2112/metrics
#   Prometheus:    http://localhost:9090
#   Grafana:       http://localhost:3000（匿名访问，自动加载 Dashboard）
```

目录结构（`metrics/prometheus/`）：

```
├── exporter.go              # Exporter（prometheus.Collector），11 个指标
├── exporter_test.go          # 测试
├── go.mod / go.sum          # 子模块依赖
├── example/
│   ├── main.go               # 示例：树形多分支 Pipeline
│   ├── Dockerfile            # 多阶段构建（alpine 镜像）
│   ├── docker-compose.yaml   # App + Prometheus + Grafana 一键启动
│   ├── prometheus.yml        # Prometheus 配置（scrape + 告警引用）
│   ├── alerts.yml            # 告警规则（队列积压/错误率/吞吐骤降）
│   └── grafana/
│       ├── datasources/      # 自动配置 Prometheus 数据源
│       └── dashboards/       # 预制 Dashboard（7 张面板）
└── README.md

## 指标清单

| 指标 | 类型 | Labels | 说明 |
|------|------|--------|------|
| `pipeline_stage_processed_total` | Counter | `stage` | 处理总数 |
| `pipeline_stage_errors_total` | Counter | `stage`, `code` | 错误分类（timeout/invalid_input/processing/system/unknown） |
| `pipeline_stage_throughput` | Gauge | `stage` | 每秒处理条数（相对上一帧） |
| `pipeline_stage_latency_avg_ns` | Gauge | `stage` | 平均延迟 |
| `pipeline_stage_latency_p50_ns` | Gauge | `stage` | P50 延迟 |
| `pipeline_stage_latency_p99_ns` | Gauge | `stage` | P99 延迟 |
| `pipeline_stage_latency_max_ns` | Gauge | `stage` | 最大延迟 |
| `pipeline_stage_queue_depth` | Gauge | `stage` | 输入队列积压 |
| `pipeline_stage_blocked_ns_total` | Counter | `stage` | 累计阻塞耗时 |
| `pipeline_stage_route_accepted_total` | Counter | `stage` | 路由投递数 |
| `pipeline_stage_route_rejected_total` | Counter | `stage` | 路由过滤数 |

## 依赖

- `github.com/YUJIAJING0408/pipeline`（主库）
- `github.com/prometheus/client_golang`（第三方）

## 设计原则

- **零侵入主库**：不修改主库一行代码，基于 `Monitor.Metrics()` 快照轮询
- **独立子模块**：`go get` 时仅拉取子模块依赖，主库保持零依赖
- **与 MetricsServer 共存**：一个提供人类面板，一个提供 Prometheus 数据源