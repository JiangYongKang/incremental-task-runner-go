# incremental-task-runner-go

本地增量任务执行与结果缓存组件。支持带依赖关系的任务并发调度、按输入与依赖结果计算标识的结果缓存，
并对失败、取消、超时给出可区分、可复现的结论。

- 包路径：`github.com/highcumontoa/incremental-task-runner-go`（包名 `runner`）
- 仅使用 Go 标准库，Go 版本要求见 `go.mod`。

## 快速开始

```go
r, err := runner.New([]*runner.Task{
    {ID: "compile", Input: []byte("src-v1"), Run: compile},
    {ID: "package", Deps: []runner.TaskID{"compile"}, Run: pack},
},
    runner.WithConcurrency(4),                  // 整体并发上限
    runner.WithCache(cache),                    // 复用同一缓存即可跨 Run 增量续跑
    runner.WithLogger(runner.NewLogger(os.Stdout)),
    runner.WithDefaultTimeout(30*time.Second),  // 任务默认超时（任务级 Timeout 优先）
)
if err != nil {
    // 声明期问题在这里被拒绝（见“声明校验”）
    log.Fatal(err)
}
rep, _ := r.Run(ctx)
for _, id := range rep.Order {
    res := rep.Results[id]
    fmt.Println(id, res.Status, res.Cached, res.Err)
}
```

任务函数签名：

```go
type TaskFunc func(ctx context.Context, in TaskInput) (TaskOutput, error)
// TaskInput.Input  为该任务声明的输入副本
// TaskInput.Deps   为各直接依赖任务的成功结果（按依赖 ID 索引，均为副本）
```

## 调度规则

1. **声明校验（`runner.New` 阶段，问题不静默跳过）**，错误类型可按原因区分：
   - `*DuplicateTaskError`：任务 ID 重复声明；
   - `*MissingDependencyError`：依赖了不存在的任务（含缺失的 ID）；
   - `*CycleError`：依赖成环（含环上 ID 序列，自依赖也算环）；
   - `*InvalidTaskError`：空 ID、`Run` 为 nil、并发上限非正、超时为负等。
2. **分层调度**：任务在所有直接依赖结束后才开始；拓扑顺序由 Kahn 算法生成，
   可入队节点按 ID 字典序选取，因此同一份声明的调度顺序确定、可复现。
3. **同层并发、整体限流**：没有依赖关系约束的任务可并发执行，但同一时刻实际在执行的任务数
   不超过 `WithConcurrency(n)`（默认 `runtime.NumCPU()`）。等待并发槽本身也参与取消判定。
4. **结论分类（`TaskResult.Status`）严格区分**：
   - `succeeded`：成功（实际执行或缓存命中，对外结论一致）；
   - `failed`：任务函数返回了非超时、非取消的错误；
   - `timed_out`：任务级 `Timeout` 到期，或父 context 整体 deadline 到期；
   - `canceled`：父 context 被主动取消；
   - `upstream_failed`：某个直接依赖不是 `succeeded`，本任务不执行；
     对应错误 `*UpstreamFailureError` 指明是哪个上游、什么状态；
   - `running`：执行中，可通过 `Runner.Snapshot()` 观测中间状态。
5. **分支隔离**：某个任务失败/超时/取消不会取消无关任务；只有其直接、间接下游会被标记
   `upstream_failed`。运行期每个任务的结论都体现在返回的 `Report` 中，`Run` 不因任务失败返回 error。

## 缓存失效条件

每个任务的缓存条目包含三个内容指纹（SHA-256，分段带长度前缀，依赖集合按 ID 排序，与 map 顺序无关）：

- `InputFP = H(任务输入)`
- `DepsFP  = H(排序后的 [(依赖 ID, H(依赖结果)) ...])`
- `ResultFP = H(任务结果值)`

判定规则：

1. **输入变化 → 缓存失效**：`InputFP` 不一致时，旧条目不命中，任务重新执行，成功后覆盖旧条目。
2. **依赖结果变化 → 缓存失效**：任一直接依赖结果变化都会改变 `DepsFP`，下游因此级联重算；
   依赖身份不变但结果不变时，下游仍可命中。
3. **指纹一致才命中**：`InputFP` 与 `DepsFP` 同时一致才视为命中，命中时直接返回成功结论，
   `TaskResult.Cached = true`，其 `Status/Value` 与真正执行一遍完全一致。
4. **损坏/旧结果不得静默命中**：声称命中的条目若 `H(Value) != ResultFP`
   （例如存储被篡改、改了输入却被外部缓存返回旧结果），任务判为 `failed`，
   错误为 `*CacheConsistencyError`，绝不把旧结果当成功结论返回。
5. **跨 Run 续跑**：在多次 `Run` 之间复用同一个 `Cache`（默认 `MemoryCache`），
   已成功且未失效的任务直接命中缓存、不会重算；上次失败/取消/超时的任务不产生缓存，重跑时重新尝试。

## 本地验证

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全量单测（含取消/超时等时序场景）
go test -v ./...

# 竞态检测（推荐用于验证并发上限与调度实现）
go test -race ./...

# 稳定性多轮（时序类测试可重复执行）
go test -count=20 ./...
```

测试覆盖场景：

- 依赖成环、自依赖成环、重复声明、依赖缺失、非法声明（错误原因可区分）；
- 输入变更导致缓存失效并重算、依赖结果变更级联失效、跨 Run 续跑不重算、
  命中路径与执行路径结论一致、缓存损坏报 `CacheConsistencyError`；
- 实测同层并发不超过配置上限；
- 排队中取消、任务级超时、父超时与父取消状态区分、部分失败不影响无关分支、
  失败后干净续跑、`Snapshot` 观测 `running`。

测试日志通过 `WithLogger` 注入的测试 logger 输出，每条判定包含任务标识与判定依据
（如 `task b: cache invalidated (reason=input or dependency result changed; ...)`）。
