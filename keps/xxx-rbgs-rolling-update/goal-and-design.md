## API 设计总览
```
apiVersion: workloads.x-k8s.io/v1alpha2
kind: RoleBasedGroupSet
metadata:
  name: rbgs-rolling
  namespace: default
spec:
  replicas: 2

  rolloutStrategy:
    # Type selects how ONE child RoleBasedGroup is moved onto the new template.
    #   Recreate        - delete the RoleBasedGroup and rebuild it, so every downstream
    #                     workload and Pod is replaced and the group restarts as a whole.
    #                     This is the only value the controller implements today.
    #   InPlaceUpdate   - reserved; the CRD enum rejects it for now.
    type: Recreate
    
    maxUnavailable: 1
    maxSurge: 0
    partition: 0
```

## 期望新增支持的API
- type当前仅支持Recreate，InPlaceUpdate预留，未设置rolloutStrategy时与未引入本kep时的行为一致（legacy path，总是直接inplace更新所有子rbg，确保它们与rbgs模板一致）
- maxSurge、maxUnavailable、partition（暂无paused，可使用partition）
- rbgs status.condition 新增滚动升级状态
- rbgs status 的 currentRevision、updateRevision

## 要求
- 当且仅当rbgs的模板变化时，才从滚动升级完成变为滚动升级进行中。故障恢复重建等不触发该状态变化
- condition中透出滚动升级状态，涵盖滚动升级全部完成/partition完成/滚动升级中
- maxSurge 百分比取整向上；maxUnavailable 取整百分比向下。二者校验都拒绝负数。二者如果纯整数均配置为0，被webhook拦截。如果controller计算得出二者均为0，按maxUnavailable=1继续操作，不拦截
- currentRevision 只在全量完成时才推进 —— completeUpdate：UpdatedReplicas == ReadyReplicas == Replicas == *spec.Replicas 才 CurrentRevision = UpdateRevision。partition > 0 时这个条件永远不成立，所以老版本一直可读
- partition > 0 时滚动升级，新实例按新版本，旧实例被删除或者出现故障而重建等场景（ordinal <= partition），依旧按旧版本
- 当同时使用 partition 和 maxSurge 时，如果大于 partition 的实例都滚动完成，surge 实例应当销毁而不是保留
- 如果模板只改变了 roles[].replicas，不要 fall back 到 recreate，而是原地更新子RBG
- replicas、readyReplicas计数必须包括surge实例
- webhook阻止create时启用role的scaling adapter；如果update的操作是开启一个已有的role的scaling adapter或者新增一个role且启用它的scaling adapter，拒绝