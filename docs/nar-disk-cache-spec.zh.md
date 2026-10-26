# nar-disk-cache 实现规格书 (v1)

> 状态: 待实现 | 分支: `nar-disk-cache` (fork: luochen1990/selector4nix, base: c86ed6f)
> 目标: NAR 下载中途失败后, 已收字节落盘保留 (TTL 内), 后续请求从水位续传,
> 消除 "nix 无断点续传 → 整文件重下" 的流量放大事故 (宿主实证: 2026-10-25,
> 3.5min 代理断网窗 × 3.5GiB NAR = ~20GiB 浪费流量)。

## 0. 核心原则 (不可妥协)

1. **partial 字节永不直接发给客户端** — 未整文件校验的磁盘字节不出网。
   客户端只从两种来源收字节: (a) 实时上游流 (与无缓存时信任级相同, nix 端
   NarHash 终检兜底), (b) 校验通过的完整体 (磁盘直送)。
2. **store 是完整文件的唯一缓存** — 校验通过的完整体服务后即删, 不持久驻留。
   完整体的存在意义只有 "服务当前等待者" 一次性窗口。
3. **零回填** — 没有消费者时不向上下游发起任何补完下载。filler 生命周期
   与等待者绑定, 最后一个等待者断开 → filler 优雅终止, partial 原地保留。
4. **上游流量单调不重烧** — 对同一 NAR, 已落盘的 [0..W) 字节永不向上游重取
   (换源重拉仅在整文件校验失败时发生, 见 §5)。

## 1. 配置 schema

```toml
[nar_disk_cache]
enabled = false                        # bool, 默认关
dir = "/var/cache/selector4nix"        # PathBuf, 中性默认
max_size_bytes = 21474836480           # u64 字节, partial 目录总上界 (20GiB)
partial_retention_secs = 172800        # u64 秒, partial TTL (48h)
fsync_interval_bytes = 67108864        # u64 字节, meta 水位刷盘粒度 (64MiB)
```

- 对齐现有配置风格: 纯数值 (不做 "20GiB" 字符串解析), NixOS 层负责人类单位换算。
- `enabled = false` 或 `dir` 不可写 (创建失败) 时: 功能整体旁路 (log WARN 一次),
  行为与现状完全一致, **不 fail 服务**。

## 2. 磁盘布局与崩溃一致性

```
<dir>/
  partial/<narhash>.nar      # append-only 顺序字节 (tee 自消费流)
  partial/<narhash>.json     # {"expected_total": u64, "watermark": u64,
                             #  "complete": bool, "updated_at": unix_secs}
```

- 键 = narhash (NAR 文件名哈希, 内容寻址), 不分来源镜像。
- **watermark 取 `min(文件实际大小, meta.watermark)`** — meta 滞后于数据时信
  meta (尾巴截掉重来, 损失 ≤ fsync 间隔); 数据滞后于 meta (崩溃撕裂) 同理
  防虚高水位服务坏字节。
- meta 每消费 `fsync_interval_bytes` 字节 fsync 一次; 流优雅结束时 (EOF 或
  客户端断开) 最终 fsync。
- EOF 时置 `complete=true` (W==expected_total 校验一致才置, 不一致按 W 收尾)。
- 命名冲突防御: 并发同 hash 由 §4 单飞锁串行化, 磁盘文件同一时刻至多一个 appender。

## 3. 请求路径状态机

```
客户端 GET /nar/<hash>:
├─ [nar_disk_cache 禁用] → 现有行为, 零改动
├─ partial 不存在 (miss):
│     实时流服务客户端 (现状路径) + tee 追加落盘 (该 hash 的 append 许可证持有者)
│     客户端断开 → tee 停, partial 保留于当前水位
│     EOF → meta.complete=true, 文件原地保留 (W=total 的 partial,
│            供未来 GC 后复用/校验即服务)
├─ partial 存在, W < total:
│     不向客户端发任何字节, 发起 filler: 上游 Range: bytes=W- 续传追加
│     → EOF → 整文件 sha256 vs narinfo FileHash:
│        ✓ 通过 → 向等待者磁盘直送 (Content-Length=total), 打开后 unlink
│        ✗ 失败 → 删 partial, 换源全量重拉一次再校验; 再败 → 向客户端报错退出
├─ partial 存在, complete=true (W==total):
│     校验 → 通过: 磁盘直送 + unlink; 失败: 删 + 换源重拉一次 (同上)
└─ narinfo 无 FileHash/FileSize (罕见):
      本 NAR 旁路缓存 (纯实时流), 不落盘
```

## 4. 并发模型

- **per-hash 单飞**: 进程内 registry (HashMap<hash, FillHandle>)。
- partial-hit: 首请求持有 FillHandle 执行 filler; 并发后来者 attach 为等待者,
  校验通过后从同一文件句柄服务 (各自独立读 offset)。
- miss 路径并发: 同 hash 第二个并发 miss 流 **不 tee** (debug log), 正常实时流
  — 牺牲罕见竞态下的一次重复落盘机会, 换取 "至多一个 appender" 的不变量。
- 最后等待者断开 → filler 取消 (tokio CancellationToken), partial 保留。

## 5. 校验与自愈

- 完整性判据: `sha256(file) == narinfo.FileHash` (narhash 内容寻址, 校验可行)。
- 校验失败即自愈: 删除 partial → **换源** (优先未尝试过的候选 substituter)
  全量重拉 → 再校验。单次客户端请求至多重拉一次, 防死循环。
- miss 路径 EOF 不做后台校验 (无等待者, 校验无收益, 违反零回填精神)。

## 6. 驱逐

- 后台 sweep 任务 (5min 周期, 独立 tokio task):
  1) 删过期 partial (`updated_at + partial_retention_secs < now`)
  2) 目录总量 > `max_size_bytes` 时按 `updated_at` LRU 删至达标
- 跳过 registry 中活跃 (in-flight) 的 hash。
- sweep 与请求路径的删除竞争: unlink 幂等, ENOENT 忽略。

## 7. 集成锚点 (当前代码库)

- HTTP 入口: `crates/selector4nix-web/src/router.rs` `get_nar` →
  `StreamNarFileUseCase` (`crates/selector4nix-core/src/application/usecase/nar_file/stream.rs`)
- 流获取: `ReqwestNarStreamProvider::stream_nar`
  (`crates/selector4nix-core/src/infrastructure/provider/nar_stream_provider.rs`)
  — filler 复用其候选/Range 机制, 起始 offset 改为水位 W
- 分块流: `crates/selector4nix-streaming/src/stream/chunked.rs`
  (chunk 级续传已内建, filler 直接受益)
- narinfo 模型: `crates/selector4nix-core/src/domain/nar_info/model/`
  (FileHash/FileSize 字段)
- 配置: `crates/selector4nix-core/src/infrastructure/config/general_raw.rs` +
  `general_parsed.rs` + `docs/configuration.md` + `docs/selector4nix.example.toml`
- NixOS 模块: 仓内 flake 的 `nixosModules.selector4nix` —
  新增 `services.selector4nix.settings.nar_disk_cache.*` 透传 (attrset,
  对齐现有 settings 风格)
- 新模块建议落位: `crates/selector4nix-core/src/infrastructure/nar_cache/`
  (storage.rs + supervisor.rs + mod.rs), 领域逻辑经 port 注入, 便于 mock

## 8. 可观测性

- INFO: `nar cache partial retained` (hash, watermark) / `nar cache resumed`
  (hash, from, total) / `nar cache verified & served` (hash, size) /
  `nar cache sweep` (deleted_n, reclaimed_bytes)
- WARN: `nar cache verify failed, evicting & refetching` (hash, expect, actual)
  / `nar cache bypass` (reason)
- 计数器挂现有 metric 体系 (`infrastructure/metric`), 缓存命中/续传字节数。

## 9. 测试要求

单测 (纯逻辑): watermark min 规则 / meta serde 兼容旧缺字段 / TTL-LRU sweep
排序 / 配置解析默认值。

集成 (复用 `tests/integration/mock/nar_stream_provider.rs` 形态):
- T1 miss 中途断开 → partial 保留且 watermark 正确
- T2 partial-hit → mock 断言上游收到 `Range: bytes=W-`
- T3 续传完成 + 校验通过 → 客户端收到全量字节, 文件被 unlink
- T4 校验失败 → partial 被删, 换源重拉后成功
- T5 并发双 miss → 仅一路落盘, 双客户端均收到完整流
- T6 最后等待者断开 → filler 取消, 无进一步上游请求

## 10. 非目标 (v1 明确不做)

- range 位图稀疏 partial (乱序块保留) — 顺序前缀已覆盖 99% 价值
- 按填充速率节流的前缀服务 (消 nix 300s 停滞线的收敛轮次)
- dashboard UI / 手动驱逐端点 (rm 文件即逃生口)
- complete 持久保留 / LAN 共享

## 11. DoD

- `cargo build` + `cargo test` 全绿, `cargo clippy -- -D warnings` 干净
  (上游 CI 标准)
- §9 全部用例存在且通过
- 文档: `docs/configuration.md` + example.toml 增补 nar_disk_cache 节
- 代码注释与文档用英文 (上游 PR 目标); 本规格书中文留存于 fork 分支
