# RBGSet 滚动升级：控制器实现说明

本文说明 `RoleBasedGroupSet` 控制器如何实现滚动升级，面向需要审阅代码（而非了解用户可见行为）
的读者。API 结构与字段级文档见
[rolebasedgroupset_types.go](../../api/workloads/v1alpha2/rolebasedgroupset_types.go)，
可运行示例见
[rbgs-rolling-update.yaml](../../examples/basic/rbgs/rbgs-rolling-update.yaml)。

全部逻辑集中在一个文件：
`internal/controller/workloads/rolebasedgroupset_controller.go`。
准入校验在 `api/workloads/v1alpha2/rolebasedgroupset_admission.go`。

> 英文版：[rbgset-rolling-update-implementation.md](rbgset-rolling-update-implementation.md)

## 1. 代码地图

| 函数 | 职责 |
|------|------|
| `Reconcile` | 入口：读取 set、列出子对象、选择路径、更新 status。 |
| `classifyChildren` | 按序号把子对象分为 `base` / `surge` / `invalid`。 |
| `parseGroupOrdinal`、`ordinalOf` | 解析 `groupset-index` 标签。 |
| `resolveRollingParams` | 把 `spec.rolloutStrategy` 的 IntOrString 字段解析成整数。 |
| `reconcileStatic` | 旧路径，`spec.rolloutStrategy` 未设置时使用。 |
| `reconcileRolling` | 滚动路径：六个有序步骤。 |
| `recreateOutdatedGroups` | 不可用预算与删除决策。 |
| `rolloutComplete` | 滚动是否完成，包含就绪判定。 |
| `isReady`、`isServing` | 全局唯一的就绪定义。 |
| `rolesEqual`、`onlyReplicasChanged` | 两个模板比较谓词。 |
| `scaleUp`、`scaleDown` | 创建与删除子对象。 |
| `updateExistingRBGs`、`syncChildrenMetadata`、`syncRBGMetadata` | 原地传播。 |
| `needsTemplateLabelUpdate`、`needsTemplateAnnotationUpdate`、`isSystemManagedMetadataKey` | metadata 漂移检测。 |
| `updateStatus` | 副本计数与三个 condition。 |
| `newRBGForSet` | 为给定序号构造子对象。 |

## 2. 读走缓存，删除用 UID 钉住

reconcile 路径只有一个 reader：

```go
type RoleBasedGroupSetReconciler struct {
    client    client.Client   // delegating: reads hit the informer cache, writes go to the API server
    apiReader client.Reader   // mgr.GetAPIReader(), used only by CheckCrdExists at startup
    scheme    *runtime.Scheme
    recorder  record.EventRecorder
}
```

`client` 由 `utilclient.NewClientWithUserAgent` 构造，其中设置了
`CacheOptions.Reader = mgr.GetCache()`。reconcile 路径上的每一次读都经过 informer，所以任何
一轮 reconcile 都不产生未缓存的 API 调用。`apiReader` 之所以还留在结构体上，是因为
`CheckCrdExists` 在任何缓存同步完成之前就需要它；除此之外没有别的使用者。

在缓存快照上行动之所以安全，靠的是两个互相加强的理由。

**降序遍历吸收了滞后。** 删除一个子对象会触发一串 watch 事件（子对象本身、它的
`RoleInstanceSet`、它的 `RoleInstance`、它的 Pod），每一个都会通过 `Owns(&RoleBasedGroup{})`
把父 set 重新入队。这些 reconcile 可能在 informer 送达那次删除之前就跑起来，于是快照里那个组
仍然存在并且仍在服务。但候选列表按序号降序排列，所以那个「删除尚未被观察到」的组正好是第一个
候选：预算被花在再删它一次上（这是个 no-op），而仍在服务的更低序号因此得到保护。
**快照越旧，这一轮越保守。**

| 快照里刚被删的那个组显示为 | `unavailableBase` | 第一个候选 | 结果 |
|---|---|---|---|
| serving、旧模板（事件还没送达） | 0 | 就是它，`deletionTimestamp` 为空 | 再删一次，no-op；预算耗尽，更低序号安全 |
| Terminating | 1 | 就是它，被 `continue` 跳过 | 预算已耗尽，break |
| 已消失 | 1（来自缺失序号） | 下一个更低序号 | 预算已耗尽，break |
| 已被替换、未就绪 | 1 | 下一个更低序号 | 预算已耗尽，break |
| 已被替换且就绪 | 0 | 下一个更低序号 | 删除，这是正确行为 |

**UID precondition 补上降序覆盖不到的那一种。** 降序救不了的唯一一种旧快照，是滞后跨越了整个
「删除 → 重建 → 就绪」周期的快照：它还显示旧对象在旧模板上，而 API server 里已经有一个在跑
新模板的替代对象。`typedClient.Delete` 只按名字定位目标，所以那次删除会删掉替代对象。因此
每次删除都钉住它推理所依据的 UID：

```go
uid := rbg.UID
err := r.client.Delete(ctx, rbg,
    client.PropagationPolicy(metav1.DeletePropagationForeground),
    client.Preconditions{UID: &uid},
)
```

UID 不匹配会返回 409 conflict，并被丢弃：正在进行的那次删除本来就会把 set 重新入队，下一轮
就能在看见替代对象的情况下重新决策。这不产生额外 API 调用，而且严格强于权威重读——因为校验
在 API server 上是原子的，而重读得到的新快照本身也可能在删除落地之前再次变旧。

`scaleDown` 带同样的 precondition。它处理 invalid 子对象和被回收的 surge 组，而那里丢掉的序号
会在后续步骤中按名字回填，所以暴露面是一样的。

## 3. Reconcile 主流程

```text
Reconcile(req)
  |
  +-- Get rbgset [缓存]
  |     \-- DeletionTimestamp 已设置? --> return，清理由 owner 负责
  |
  +-- 按 groupset-name=<set 名> 列出子对象 [缓存]
  |
  +-- replicas = *spec.replicas
  +-- children = classifyChildren(list, replicas)
  |
  +-- spec.rolloutStrategy == nil ?
  |     +-- 是 --> reconcileStatic(ctx, rbgset, replicas, children)
  |     \-- 否 --> reconcileRolling(ctx, rbgset, replicas, children,
  |                                resolveRollingParams(rbgset))
  |
  +-- 再次列出子对象 [缓存]
  \-- updateStatus(ctx, rbgset, list)
```

第二次 list 是刻意的：前面的步骤会创建和删除子对象，status 必须描述本轮 reconcile 的
结果，而不是它的输入。

## 4. 子对象分类

子对象由 `newRBGForSet` 写入的两个标签标识：

| 标签 | 值 | 用途 |
|------|-----|------|
| `rbg.workloads.x-k8s.io/groupset-name` | set 的名字 | list 的 selector |
| `rbg.workloads.x-k8s.io/groupset-index` | 序号，无零填充 | 排序与分桶 |

```text
classifyChildren(list, replicas)
  |
  +-- index 标签缺失、不可解析或为负 --> invalid（在步骤 1 删除）
  +-- ordinal >= replicas --> surge
  \-- ordinal < replicas --> base
```

`base` 和 `surge` 都是以序号为 key 的 map，所以"某个序号缺失"就等于"map 里少一个 key"。
这正是步骤 3 能够精确填补那个空洞、而不是把后面所有组往前挪的原因：删掉 `-1` 之后新建的
仍然是 `-1`，绝不会变成重命名后的 `-2`。

没有 surge 标签。surge 组只靠序号区分，这与控制器内部的判定方式完全一致，而且它们
和 base 组一样承接流量。

## 5. 参数解析

`resolveRollingParams` 把 IntOrString 字段按 `spec.replicas` 解析：

| 字段 | 百分比取整方向 | 范围约束 | 默认值 |
|------|---------------|---------|--------|
| `maxSurge` | 向上 | 无 | `0` |
| `maxUnavailable` | `maxSurge == 0` 时向上，否则向下 | 仅当 `maxSurge == 0` 时下限抬到 `1` | `1` |
| `partition` | 向下 | 收敛到 `[0, replicas]` | `0` |

两个细节值得注意：

- `maxUnavailable` 可以合法地解析为 `0`，但只在存在 surge 预算时才行。此时就绪的 surge 组
  提供了那份容量，使得重建一个 base 组的过程中可用数不会掉到 `replicas` 以下。把这个 `0`
  抬到 `1` 会丢掉用户明确要求的语义，所以只有在没有 surge 可依靠时才抬。
- `partition == replicas` 是刻意允许的。它把所有已存在的组都挡住，配合 `maxSurge > 0`，
  surge 组就成为跑在新模板上的常驻金丝雀。

非法值在这里回落到默认值而不是直接拒绝；正常情况下准入校验会先拦住它们。

## 6. 滚动路径：六个步骤

`reconcileRolling` 先把 base 子对象分到三个桶里，然后执行六个有序步骤。顺序本身就是设计：
回收发生在创建之前，创建发生在删除之前，所以单轮 reconcile 不会在无从判断的情况下
既撤回容量又花费容量。

```text
对每个 base 子对象:
  +-- rolesEqual(spec.roles, template) --> 只检查 metadata 漂移
  +-- onlyReplicasChanged(spec.roles, ...) --> scaleOnly（原地处理，绝不重建）
  \-- 其他 --> outdated（重建候选）

inProgress = len(outdated) > 0
complete = rolloutComplete(rbgset, children, replicas)
```

| 步骤 | 动作 | 触发条件 |
|------|------|---------|
| 1 | 删除 `invalid` 子对象 | 总是执行 |
| 2 | 回收序号 `>= replicas+maxSurge` 的 surge 组，或在 `complete` 时回收全部 | 总是执行 |
| 3 | 为 `[0, replicas)` 中每个缺失序号创建子对象 | 总是执行，暂停时也执行 |
| 4 | 在 `[replicas, replicas+maxSurge)` 创建 surge 组 | `!paused && inProgress && len(keptSurge) < maxSurge` |
| 5a | 原地应用 `scaleOnly` | 总是执行，暂停时也执行 |
| 5b | 原地同步模板的 labels 与 annotations | `!paused` |
| 6 | 在预算内重建 `outdated` 组 | `!paused && len(outdated) > 0` |

扩缩容（步骤 3 和 5a）既不受节奏控制，也不会被冻结。调和 `replicas` 与传播模板是两件
不同的事，所以暂停中的滚动仍然会扩缩容，只改副本数的模板编辑也仍然立即生效，而不是
重建整个组。

步骤 6 只负责删除。被删掉的序号由**后续**某一轮 reconcile 的步骤 3 补回。把删除与创建
拆到不同的 reconcile，正是节奏可被观察到的原因：一个已经消失但还没被重建的组，就是一个
缺失的 map key，步骤 3 会把它变成一个基于当前模板的新对象。

## 7. 删除决策

可用性保证的全部内容都在这里。

```text
recreateOutdatedGroups(ctx, rbgset, replicas, children, outdated, params)
  |
  +-- children 与 outdated 就是本轮 reconcile 自己的分类结果，从不重读
  |
  +-- unavailableBase = replicas - len(children.base)          # 缺失的序号
  +-- 对每个 base 子对象: !isServing(child)          --> unavailableBase++
  +-- readySurge = isServing 为真的 surge 子对象个数
  +-- 按序号降序排序 outdated
  +-- budget = params.maxUnavailable + readySurge
  |
  \-- 对 outdated 中的每个候选:
        +-- ordinal < params.partition       --> break   # 更低序号都被挡住
        +-- DeletionTimestamp 已设置         --> continue # 正在退出，已计入不可用
        +-- unavailableBase >= budget        --> 打印 "Unavailability budget exhausted" 并 break
        \-- Delete(foreground, Preconditions{UID}) + Event "RecreatingGroup" + unavailableBase++
```

审阅时通常会被问到的四点：

- **缺失的序号计入不可用。** 一个在上一轮被删掉、还没重建的组，就是不在位的容量，
  和一个没就绪的组完全等价。少了这一条，预算每一轮都会从零重新计算，于是若干个组会
  被一次性删掉。
- **正在终止的组计入不可用。** `isServing` 要求 `deletionTimestamp` 为空，所以处于
  foreground 删除过程中的组会持续占用预算，直到它真的消失。
- **序号降序**让滚动过程确定，并与 StatefulSet 的惯例一致：序号最大的先走。
- **`partition` 用 break 而不是 skip。** 因为列表已按降序排列，遇到第一个低于
  `partition` 的序号，就意味着剩下的候选全都低于它。

`readySurge` 会扩大预算，因为就绪的 surge 组是真实的服务容量。这正是
`maxUnavailable: 0` 可用的原因：配合 `maxSurge: 1`，预算变成 `0 + 1`，于是可以重建一个
base 组，同时由 surge 组顶替它。

## 8. 三个谓词

### `isServing`

```go
func (r *RoleBasedGroupSetReconciler) isServing(rbg *workloadsv1alpha2.RoleBasedGroup) bool {
    return r.isReady(rbg) && rbg.DeletionTimestamp.IsZero()
}
```

这是控制器里**唯一**的就绪定义。所有依赖就绪的决策都经过它：不可用预算、用于扩大预算的
surge 计数、完成判定，以及对外上报的 `readyReplicas`。`isReady`（裸的 `Ready` condition）
是 `isServing` 的内部实现细节，别处不再调用。

`deletionTimestamp` 那一半不是可选的。用 foreground 级联删除的组会一直可见，而且它的
`Ready` condition 仍然是 true，持续时间等于它的 Pod 走完 preStop hook 和 grace period
所需的时间。把这样的组算作就绪，会在整个窗口内高估可用性——对一个配了 45 秒 preStop hook
的 Pod，实测这个窗口就是 45 秒。

### `rolesEqual`

把两个切片各拷贝一份，分别按 role 名排序，然后 `reflect.DeepEqual`。排序让比较不受用户
列举 role 的顺序影响，所以调整模板里 role 的顺序不会重建所有组。

### `onlyReplicasChanged`

当子对象与模板拥有相同的 role 名集合，且除 `Replicas` 外每个字段都相同时为真。实现方式是
拷贝子对象的 role，把它的 `Replicas` 指针覆盖成模板的值，再比较：

```go
cc := *c                        // shallow copy of the child role
cc.Replicas = template[i].Replicas
if !reflect.DeepEqual(cc, template[i]) { return false }
```

这种差异属于扩缩容而不是滚动，所以由步骤 5a 原地应用。仅因为某个 role 改了副本数就重建
整个组，会让组里所有 Pod 白白重启一次。

## 9. 完成判定与 surge 生命周期

```go
func (r *RoleBasedGroupSetReconciler) rolloutComplete(...) bool {
    if len(children.base) != replicas { return false }
    for _, rbg := range children.base {
        if !r.isServing(rbg) || !r.rolesEqual(rbg.Spec.Roles, template) { return false }
    }
    return true
}
```

三个条件，缺一不可：每个 base 序号都**存在**、都在**当前模板**上、并且都在**服务**。

就绪这一项是让 surge 预算撑到最后的关键。一个刚被重建的组，在它存在的那一刻就已经匹配
模板了，所以只比较模板是否相等，会导致在这个组还没起来的时候就回收 surge 组，可用数会
在新组启动所需的整段时间里低于 `replicas - maxUnavailable`。

完成判定基于缓存快照，并且刻意不重读。缓存只会滞后，不会领先：它持有的总是 API server
真实存在过的某个状态，而一个「每个 base 组都已在服务当前模板」的历史状态，之后不会变得
更不完整。因此快照落后的每一种可能，都只会延后回收，绝不会提前回收。

| 快照里那个刚重建的组显示为 | `rolloutComplete` |
|---|---|
| 旧对象、serving、旧模板 | false，`rolesEqual` 不过 |
| 旧对象、Terminating | false，`isServing` 不过 |
| 完全不存在 | false，`len(base) != replicas` |
| 新对象、未就绪 | false，`isServing` 不过 |

surge 组只在两种情况下被回收：序号超过 `replicas + maxSurge`（这也覆盖了调低 `maxSurge`
的情形），或者滚动已完成。在滚动未完成期间它们持续服务，包括在某个被重建的 base 组还在
启动的时候。暂停时它们被保留而不是丢弃，因为重建整个组代价高昂，在恢复时把这份工作
扔掉是浪费。

## 10. 为什么重建路径的删除用 foreground

```go
r.client.Delete(ctx, rbg, client.PropagationPolicy(metav1.DeletePropagationForeground))
```

子对象是用**同一个确定性名字**重建的，这让级联策略从"修饰性选择"变成了"承重结构"。

| 级联策略 | 发生什么 | 在本场景下的后果 |
|---------|---------|-----------------|
| background（默认） | `RoleBasedGroup` 对象立刻消失；它的 `RoleInstanceSet`、`RoleInstance` 和 Pod 被异步 GC | 在这个窗口里创建的替代对象会收养这些残留物，并据此上报 `Ready`，于是在第一个组真正存在之前，预算就被释放给第二个组 |
| **foreground** | 对象带着 `deletionTimestamp` 持续可见，直到整条依赖链消失 | 该组持续被计入不可用，它的序号也不会被提前回填 |

在 background 策略下，实测两组的 set 出现过两组同时不可用。改成 foreground 之后，序列
严格串行：旧组可见且正在终止、消失、以新 UID 创建、真正就绪，然后才动下一个序号。

与 `scaleDown` 的不对称是刻意的。`scaleDown` 处理的是 invalid 子对象和被回收的 surge 组，
它们的序号不会马上被同名对象回填，所以那里用默认的 background 就够了，也避免了为没有
收益的事情去等待整条依赖链。

## 11. metadata 同步与系统自有 key

步骤 5b 把 `spec.groupTemplate.labels` 和 `.annotations` 传播给那些 spec 已经匹配模板的
子对象，且不碰它们的 spec。

两个漂移判定谓词都会忽略项目前缀下的 key，而 `syncRBGMetadata` 会把这些 key 从子对象上
沿用过来，而不是从模板重建整个 map：

```go
func isSystemManagedMetadataKey(key string) bool {
    return strings.HasPrefix(key, constants.RBGPrefix)   // "rbg.workloads.x-k8s.io/"
}
```

这不是风格偏好。`RoleBasedGroup` 控制器会在它自己的对象上记录
`rbg.workloads.x-k8s.io/discovery-config-mode`，而且只在发现它缺失时才写入。一次替换整个
annotation map 的同步会把它删掉，于是那个控制器再写回来，于是本控制器又看到漂移，两边
永远无法稳定：子对象被反复更新，它的下游 workload 反复重算 hash，它的就绪状态随之抖动。

同步仍然拥有的部分：

| key 类型 | 行为 |
|---------|------|
| `rbg.workloads.x-k8s.io/*` | 从子对象沿用，绝不删除 |
| 模板中列出的 key | 应用，覆盖子对象上原有的值 |
| `groupset-name`、`groupset-index` | 最后重新写入，所以模板无法劫持子对象的身份 |
| 其他不在模板中的 key | 删除，所以从模板里移除一个 label 确实会传播下去 |

## 12. status 与 condition

`updateStatus` **只统计 base 子对象**。surge 组是滚动过程中的临时容量，被排除在所有计数
之外，这样这些数字才能与 `spec.replicas` 直接比较，scale 子资源也才诚实。

| 计数字段 | 定义 |
|---------|------|
| `replicas` | 已存在的 base 子对象数 |
| `readyReplicas` | `isServing` 为真的 base 子对象数 |
| `currentReplicas` | **不在**当前模板上的 base 子对象数 |
| `updatedReplicas` | 在当前模板上的 base 子对象数 |
| `updatedReadyReplicas` | 在当前模板上**且**正在服务的数量 |
| `expectedUpdatedReplicas` | `replicas - partition`；未设置策略时为 `0` |

Condition：

| 类型 | 为 True 的条件 | Reason |
|------|--------------|--------|
| `Ready` | `readyReplicas >= replicas` | `AllReplicasReady` / `ReplicasNotReady` |
| `Rolling` | `currentReplicas > 0` | `RolloutInProgress`，"N still on the previous template" |
| `Rolling` | 全部已在新模板上，但 `baseTotal < replicas` 或 `updatedReady < updated` | `RolloutInProgress`，"waiting ... to become ready (x/y)" |
| `Rolling` | 以上都不满足 | `False` / `RolloutComplete` |
| `Paused` | `paused && currentReplicas > 0` | `RolloutPaused` / `RolloutNotPaused` |

`RolloutComplete` 要求就绪，而不只是模板相等，所以它永远不会与紧邻的 `Ready` condition
互相矛盾。

`spec.rolloutStrategy` 未设置时，`Rolling` 和 `Paused` 这两个 condition 会被整个移除，
而不是留着过期的值。

写入前会用 `reflect.DeepEqual(old, new)` 判断是否跳过，并且整体包在 `RetryOnConflict` 里、
每次重试都重新读取该 set，因为 status 子资源会与其他修改该对象的东西竞争。

## 13. 旧路径（static）

未设置 `spec.rolloutStrategy` 时，`reconcileStatic` 复现引入滚动升级之前的行为：

```text
reconcileStatic
  |
  +-- 删除 invalid 子对象，以及所有 surge 子对象（序号 >= replicas 视为缩容残留）
  +-- 收集 needsUpdate(...) 为真的 base 子对象
  +-- 为 [0, replicas) 中每个缺失序号创建子对象
  |
  +-- 先 scaleDown（避免更新那些即将被删除的对象）
  +-- updateExistingRBGs（原地：spec.roles 由模板整份覆盖，metadata 同步）
  \-- scaleUp
```

没有顺序、没有预算、没有就绪门控：所有过期的组在单轮 reconcile 内被原地更新。滚动升级
严格是 opt-in 的，控制器升级既不会重启、也不会"收养"那些没有申请该特性的 set。

`needsUpdate` 等于 `!rolesEqual || labelDrift || annotationDrift`，并且只在这里使用。
滚动路径改为分三个桶，因为它必须把"只改副本数"与"真正的模板变更"区分开。

## 14. 准入校验

`RoleBasedGroupSetValidator.validateSpec` 在 create 和 update 两个入口都会执行：

| 校验项 | 规则 |
|-------|------|
| `validateNoDeprecatedWorkloadTypes` | 除非打开 deprecated types 开关，否则拒绝 |
| `validateNoRoleScalingAdapter` | `spec.groupTemplate.spec.roles` 中任何 role 都不得启用 `scalingAdapter` |
| `validateGroupSetRollout` | `maxSurge` 与 `maxUnavailable` 不得同时解析为 `0`；`partition` 不得超过 `replicas`，但允许相等 |

scaling adapter 这条规则存在的原因是：set 拥有每个子对象的 `spec.roles`，并且会用模板整份
重写它。`ScalingAdapter` 会把 `replicas` 写回子对象，set 又把它改回去，而
`RoleBasedGroup` 的准入 webhook 会拒绝这次改回，且重试循环不把这种错误当作可重试。
需要自动扩缩的 role 应该放在独立的 `RoleBasedGroup` 里。

这里特意允许 `partition == replicas`，就是为了让常驻金丝雀能够工作。

## 15. 已知边界

- 只实现了 `Recreate`。`InPlaceUpdateStrategyType` 在 API 里与它并列声明，但 CRD 的 enum
  只接受 `Recreate`，因此目前无法请求原地滚动。
- 在 `Recreate` 下，子对象 role 级的 `rolloutStrategy` 不起作用：整个 `RoleBasedGroup`
  被删除，下游 workload 无从做任何决定。
- 没有进度超时，也没有自动回滚。回滚方式是重新应用上一版模板。
- 终止缓慢的 workload 会拉长滚动。foreground 删除要等整条依赖链，所以每个被重建的组
  除了启动时间，还要加上它的 Pod 终止时间（preStop hook 加 grace period），总时长约为
  `replicas x (终止时间 + 启动时间)`。一个永远不终止的组会让滚动无限期停住。
- `maxSurge` 要求集群能同时容纳 `replicas + maxSurge` 个组。在 gang scheduling 下，
  一个调度不上去的 surge 组只会让滚动一直等待。
- `spec.groupTemplate.spec.roles` 不得启用 `scalingAdapter`，见第 14 节。

## 16. 可供审阅者核对的不变量

1. 所有就绪判定都经过 `isServing`；`isReady` 没有其他调用方。
2. 任何一轮 reconcile 都不在 informer 缓存之外读取。每次删除都钉住它分类时的 UID，所以
   过时快照不可能删掉替代对象。
3. `unavailableBase` 统计缺失序号、未在服务、以及已在终止中的组。除此之外没有任何东西
   消耗预算。
4. 同一个序号的删除与重建绝不会发生在同一轮 reconcile 里。
5. 重建路径的删除是 foreground；其他所有删除是 background。
6. 子对象上 `rbg.workloads.x-k8s.io/` 前缀的 key 绝不会被模板同步移除。
7. surge 组绝不进入任何 status 计数。
8. `RolloutComplete` 蕴含 `readyReplicas == replicas`。
9. 扩缩容与只改副本数的变更，绝不会被 `paused` 或预算阻挡。
10. 未设置 `spec.rolloutStrategy` 时，`reconcileRolling` 的任何代码路径都不可达。
