# Split / relayer 503 重试与终局恢复

maker02 在 relayer 返回 `503 relayer is paused` 后，将两笔没有 job ID 的 split 留在
Uncertain。恢复代码只接受成功/失败的正向结果，没有结束“签名已过期且未执行”的路径，
并且重复写入相同未决状态。此修改为该场景增加可核验的终局收口，而不按超时释放资金。

## 请求与恢复状态机

1. POST 前仍持久化 signer、deposit wallet、nonce、签名 body hash、signature deadline。
   新增可选 `retry_until_secs`，记录策略允许重新发送请求的业务截止。
2. 503、429、响应丢失、缺少 transactionID 都视为结果未知。短重试仍使用同一 nonce/body，
   POST 及 GET 均有 5 秒 HTTP 超时，重试前先查原请求。明确 relayer paused 使用 5 秒起的退避与抖动。
3. 后续维护调用在原业务截止前，可以从原 journal 重建同一签名 body；只有 hash 完全一致才允许重新 POST。
   不分配新 nonce，不延长签名 deadline。对账耗时越过业务截止也禁止再 POST。
4. 原 relayer action 已存在时采用原 ID；没有 ID 的索引结果仍保持未知，不能被误归类为确定失败。
5. 业务截止之后只查询，不补做该场 split。没有新字段的历史 journal 也保持查询模式。
6. 请求签名过期后，按下述 nonce 证明结束确定未执行的操作；失败的 RPC、空索引和普通超时都不能释放预留。
7. 仍未知的结果继续保留原预留与既有账户准入保护，并在现有每账户 wallet actor 中退避恢复：
   5 秒起指数增长，最多 60 秒再加不足 1 秒的账户抖动。退避期间不解析 Gamma target 或发起网络查询。

该方案自动解除“已有终局证据却永远未决”的阻断。证据不可取得时保留保护；
不将结果未知的现金或库存当作可用资产。单笔资源隔离后的其余余额交易，需要先有策略所有者确认的
维护资源 lease；当前历史 StrategyAccount 没有该握手，不能通过清除共享账户 uncertain 来代替。
这条迁移路径不增加本次报价热路径的共享最终准入门。

## 可核验的未执行证明

[链上已验证 DepositWallet 源码](https://sourcify.dev/server/v2/contract/137/0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB?fields=sources)
的 execute 原子地校验并递增 nonce、检查签名 deadline、执行 batch；任一失败会回滚该交易。
源码验证记录为 chain=137、runtime/creation exact_match、源文件 SHA256
`16f0e82589aab09a41bf06f81941f3fc87e2e6393747c9e1555c07d644a10279`。

因此，对配置的 DepositWallet 协议，在**时间已越过签名 deadline 的 finalized 区块 H**，
若 `owner(H)` 仍匹配持久化 signer 且 `nonce(H) <= signed_nonce`，该签名 batch 从未成功执行，
以后也不能执行。恢复使用 Polygon chain ID 检查和 EIP-1898 的同一 `blockHash + requireCanonical`，
读取 owner/nonce 后再次核对区块号对应的 canonical hash。证明区块号、hash、时间、nonce、deadline 写入操作终态 detail。

若 nonce 已经前进，则不据此判定本笔 split 成功或失败。正向链上 fallback 需同时匹配
同一交易中的 wallet `BatchExecuted(nonce)` 和 adapter `PositionSplit(wallet, condition, amount)`，
并检查 receipt 成功、finalized 高度与 canonical block hash。旧的事件查询按不超过 10 个区块分段，
避免 maker02 免费 RPC 的范围限制。

不支持 finalized / EIP-1898 的节点会对相同只读请求尝试其他配置节点，不降级为 latest。
该能力切换只适用于 canonical 只读查询，不重试交易广播。

## 幂等、积压与账户隔离

- 相同 owner、condition、金额的队列重试，只形成一份 seed 预留及一次 split 金额；不同 owner 仍可合并。
  同 owner 金额冲突拒绝整组；不同账户或不同账本对象不合并。
- 终局 Failed 由账户 owner 应用并 flush 后，才继续下一次提交；只清理 signer 上匹配的旧 nonce 缓存，
  不清理其他 signer 或更大的 nonce。
- 相同 Uncertain 重复观测不更新 WAL；迟到的 Uncertain 不能复活 Confirmed/Failed。
- relayer 长期停在 STATE_NEW 的全局接受协调等待改为 10 秒预算（另加已发出 HTTP 请求的有界完成时间）。
  超时保留已持久化 ID，交给恢复流程，不永久占用其他账户的协调 lease。
- 保留原 condition/owner 级 `InventorySeedResult` 通道。只有截止已过且目标 journal 无未决操作、无应重放的已确认库存时，
  才发布 `ClosedWithoutInventory`；普通 FailedOrPending 状态不能伪造关闭证明。

## 所有权与队列审查

| 新状态或消息 | 所有者 / 语义 |
|---|---|
| RecoveryBackoff | 原有 `poly-wallet-<account>` 后台 actor 单写，固定大小；重启重置计时，不重置 journal 或预留 |
| retry_until_secs | account owner 经既有有界控制消息持久化；POST 前 flush；serde 缺省 None 兼容历史数据 |
| ForgetNonce | 原有后台 on-chain owner，既有容量 256 的 lane；按 signer+nonce 幂等，超时/溢出报错，不能清除别人的状态 |
| InventorySeedResult | 每策略 FIFO/lossless，容量 64，后台 producer 可以背压；策略每轮最多读取 2 条 |

没有新增线程、进程全局可变表、热路径锁、热路径 RPC、同步日志或重试睡眠。
HTTP、证明核验、journal 持久化与退避均在已有 affinity-managed 后台角色中。
现有 SharedAccount 冷维护预留接口保持原所有权，未新增其作为策略热路径的最终准入门。

验证覆盖连续 503、原响应丢失后已接受、延迟索引、签名/业务截止、空 ID、nonce 已消费、
错误 owner/chain、RPC 429/畸形/缺失 finality/reorg、预留仅释放一次、重启后下一场准入、
重复和乱序终态、其他 owner 缓存、队列溢出/排序及不同实例隔离。
测试计数与源码 SHA 见同目录 `split_relayer_recovery_20261030.json`。

本次不部署、不重启 maker02，也不手工修改其账本。资金不足仍按原规则拒绝 seed。
