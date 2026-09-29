# incremental-task-runner-go

本地增量任务执行与结果缓存组件：为有依赖关系的任务提供分层并发调度、按
「输入 + 依赖结果」计算标识的结果缓存，以及取消 / 超时 / 部分失败的可区分结论。

无第三方依赖，仅使用标准库。

## 快速开始

```go
store := taskrunner.NewMemoryStore() // 缓存可跨多次 Run / 多个 Runner 复用
r, err := taskrunner.New([]taskrunner.Task{
    {Name: "compile", Input: []byte("src-v1"), Fn: compileFn},
    {Name: "test", Deps: []string{"compile"}, Input: []byte("suite-v1"), Fn: testFn},
}, store, taskrunner.Options{MaxConcurrency: 4})
if err != nil { /* 声明非法，见下文错误表 */ }

rep, _ := r.Run(ctx)
res := rep.Get("test")
switch res.Status {
case taskrunner.StatusSuccess, taskrunner.StatusCached: // 成功结论（两条路径等价）
case taskrunner.StatusFailed:    // res.Err 为任务返回的原始错误
case taskrunner.StatusTimeout:   // errors.Is(res.Err, taskrunner.ErrTimeout)
case taskrunner.StatusCanceled:  // errors.Is(res.Err, taskrunner.ErrCanceled / ErrDependencyFailed)
}
```

## 调度规则

1. **静态校验先于执行**。`New` 会对全部声明做校验，非法声明直接拒绝，
   不会静默跳过任何任务，拒绝原因互斥且可用 `errors.Is` 区分：

   | 哨兵错误 | 触发条件 | 详情类型携带信息 |
   |---|---|---|
   | `ErrEmptyName` | 任务名为空 | — |
   | `ErrDuplicateTask` | 同名任务重复声明 | `DuplicateTaskError.Name` |
   | `ErrDependencyMissing` | 依赖了未声明的任务 | 任务名、缺失依赖名 |
   | `ErrDependencyCycle` | 依赖成环（含自依赖） | `DependencyCycleError.Cycle`（一条具体环路径） |

2. **分层调度**。通过 Kahn 算法按依赖拓扑分层：无依赖任务在第 0 层，
   其后每层只依赖更早层。**同一层任务并发执行，层间严格先后**，因此下游
   被调度时，其所有直接依赖一定已有终态结论。
3. **全局并发上限**。`Options.MaxConcurrency` 限制同一时刻**真实执行**
   （非缓存命中）的任务数；令牌在任务开始执行前获取，获取期间同样响应
   取消。`<=0` 时按 1 处理。
4. **上游闸门**。任一直接依赖不是成功结论（`failed` / `timeout` /
   `canceled`）时，该任务不会执行，状态记为 `canceled`，错误为
   `*DependencyFailedError`（可解出是哪个依赖、依赖处于什么状态）。
5. **失败只影响其下游闭包**。无关分支照常执行并保留各自结论；
   `Report.Failed()` 只列出非成功终态的任务。
6. **子集执行**。`RunSubset(ctx, targets)` 只调度目标的反向依赖闭包，
   闭包外任务不出现在 `Report` 中；配合共享缓存可从上次状态干净地继续。

## 结果标识与缓存失效

每个任务在执行前计算结果标识（`Result.Key`，SHA-256，十六进制）：

```
Key = sha256(版本头 ‖ 任务名 ‖ 任务输入 ‖ 排序后的(依赖名 → 依赖 Key) 列表)
```

所有字段带长度前缀，依赖按名称排序，因此：

- **输入变化** ⇒ Key 变化 ⇒ 缓存未命中，必须真实重算；
- **任一依赖结果变化**（依赖自身输入变化会沿依赖链传导为新的依赖 Key）
  ⇒ Key 变化 ⇒ 下游缓存失效；
- 输入与依赖均不变 ⇒ Key 稳定，多次运行结果**可复现**，且与声明顺序无关。

缓存规则：

- 命中缓存时不调用 `Fn`，但 `StatusCached` 与 `StatusSuccess` 对外等价
  （`Status.OK()` 都为 true），输出、Key 完全一致。
- 命中时会校验缓存条目自报的 Key。若存储按当前 Key 取出的条目却携带旧
  Key（即「输入变了还给出旧结果」），判定为**失败**：状态 `failed`，
  错误 `*StaleCacheError`（可解出期望 Key 与实际 Key），绝不当成功。
- **只有成功结果写入缓存**；失败 / 超时 / 取消不写缓存，因此修复后重跑
  会重新执行，而已经成功且未失效的任务不会被重算。
- `CacheStore` 为接口，默认提供并发安全的 `MemoryStore`；可替换为持久化
  实现以跨进程复用。

## 取消、超时与状态

`Task.Timeout > 0` 时为该任务设置独立截止时间；也可以直接给 `Run` 传带
超时 / 可取消的 `context.Context`。判定优先级：任务超时 → 运行被取消 /
运行级超时 → 任务返回普通错误 → 成功。

| 状态 | 含义 | 典型错误 |
|---|---|---|
| `success` | 真实执行成功 | nil |
| `cached` | 命中有效缓存，未真实执行（与 success 等价） | nil |
| `failed` | 任务返回普通错误，或命中旧标识缓存 | 原始错误 / `ErrStaleCache` |
| `timeout` | 任务级或运行级超时 | `ErrTimeout` |
| `canceled` | ctx 取消、未抢到令牌即被取消，或被失败上游阻塞 | `ErrCanceled` / `ErrDependencyFailed` |

运行期结论全部体现在 `Report` 中；`Run` 仅在无法给出逐任务结论的情况
（目前只有 `RunSubset` 目标名未知）下返回非 nil error。

## 增量继续

- 复用同一个 `CacheStore` 再次 `Run`：未失效任务直接命中缓存，失败过的
  任务会重跑，其下游在上游恢复成功后自然得以执行。
- 只关心部分目标时用 `RunSubset`，随后可再全量 `Run`，子集阶段缓存的
  结果会被全量运行复用。

## 本地验证

```bash
go build ./...
go vet ./...
go test ./... -race -count=20 -v
```

测试覆盖：依赖成环 / 缺失 / 重复声明（原因可区分）、输入变更与依赖结果
变更引起的缓存失效、旧缓存命中算失败、并发上限不被突破、取消（运行中 /
等令牌 / 下游级联三种位置）、任务级与运行级超时、失败分支隔离，以及失败
修复后的干净继续。单测日志以 `task=<名称> 判定依据=...` 格式打印任务
标识与判定理由，可用 `go test -v` 查看。

## 代码结构

```
taskrunner/
  errors.go   哨兵错误与各类可区分错误类型
  types.go    Status / Task / Result / Report / Options / CacheStore
  cache.go    ComputeKey 与 MemoryStore
  graph.go    声明校验（重复/缺失/成环）与拓扑分层
  runner.go   分层调度、并发上限、缓存判定、取消/超时分类
```
