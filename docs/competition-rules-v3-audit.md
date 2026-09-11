# V3 规则符合性审计

审计基准：`docs/competition-rules-v3.md`，来源为 `Domain_Data_Collection_Task_0909_UpdateV3.zip`。审计对象为当前 `main`（审计日期 2026-09-11）。

## 总结

当前系统的 host-year 基础模型、baseline mask、Candidate 分离和 EED 计算方向正确；来源禁用规则的主路径也已增加 fail-closed 门禁。但还不能把全部 source 结果声明为符合 V3。主要风险集中在：

1. submission builder 仍允许调用者提供 EED/growth 作为上下文；
2. EvidenceCapsule 的旧数据兼容默认值仍需在正式提交前由真实 source provenance 覆盖；
3. EED authority 尚未在没有 model path 时强制从最新 baseline 文件重算。

## 逐项结果

| 规则 | 当前结果 | 结论 |
|---|---|---|
| 精确 hostname-year 作为正式单位 | `BaselineIndex`、`EvidencePlanner`、年度导出按 year 处理 | PASS |
| 年度与 Candidate 分离 | `records/candidates.py` 有独立 scope；Common Crawl corpus 排除 | PASS |
| Common Crawl 仅用于固定 EED TLD 模型 | candidate reconciliation 和 source-discovery admission 均拒绝 Common Crawl corpus | PASS |
| hostname 规范化、年内去重 | normalizer、export dedupe、年度排序已有 | PASS |
| baseline 与 local evidence 以 mask 过滤 | `EvidencePlanner` 已实现 | PASS |
| host 已有其他年份时仍保留目标年份 | production planner 是 pair-aware | PASS |
| ISC/DNS 默认只能作候选 | ISC source scope 与 discovery-only 路径已有 | PASS |
| 未定年聚合目录不能直接进年度表 | discovery-only policy 已有，但需靠导出门禁继续保护 | PARTIAL |
| IA/Arquivo exact CDX 年份可作年度证据 | provider capsule 保存 capture timestamp 和 provenance；Arquivo source 已支持 direct mask | PASS |
| JISC/UKWA 年度 CDX 可作年度证据 | `.cdx` parser、byte-cursor production adapter 和 direct-year activation 已接入 | PASS（需真实 JISC 文件 live 验证） |
| CDXJ 明确 timestamp 作为 direct evidence | parser 保存 timestamp/direct mask；CDXJ reservoir 激活为 direct-year | PASS |
| WARC-Date 不自动等同直接证据 | 当前 WARC 路径默认 hint，符合保守政策 | PASS |
| 每条年度证据保留 source/original URL/record locator/type | `EvidenceCapsule`、SQLite、JSONL exporter 和 verifier 均有显式字段 | PARTIAL：旧兼容 capsule 使用推导值 |
| EED 从年度证据/年度文件重算 | readiness 脚本可重算，但 builder 仍接受 caller-supplied EED | PARTIAL |
| submission 前重新对最新 baseline 计算 | readiness 路径支持；正式 builder 门禁不够强 | PARTIAL |
| 六个年度文件和 Candidate 独立交付 | 两个 exporter 路径均生成六个年度文件；verifier 检查证据 provenance | PASS |

## 必须修复项

### P0 — 证据分类与解析

- 传统 CDX 行解析、original URL、精确 timestamp、record locator 和年份已完成。
- CDX/CDXJ 的明确 target-year timestamp 已设置 `direct_year_mask`；WARC metadata-only 继续保持 hint。
- JISC/UKWA、Arquivo CDX 类 adapter 已显式进入 `direct_year` production mode；需要真实文件继续做 live validation。

### P0 — provenance

`EvidenceCapsule` 和持久化 schema 已扩展，至少显式保存：

```text
evidence_type
source_file / source_id
original_url
record_locator
extraction_method
```

现有 `source_locator` 保留作为兼容字段；旧 capsule 会生成兼容性 provenance 默认值，新 production/provider 路径会显式填充。

### P1 — EED 权威链

Submission builder 不应把调用者提供的 `novel_eed` / `growth_rate` 当作最终 authority。必须从 accepted annual evidence、最新 baseline 和固定 EED model 重算，再执行 5% readiness gate。

### P1 — 交付门禁

提交导出现在检查：

- 六个年度文件均存在；
- 每条年度记录都有 exact-year evidence；
- annual/candidate 不交叉混入；
- manifest 包含 parser/policy/model/baseline 版本和 source contribution；
- EED report 与导出年度文件可重算一致。

仍待补强：无 EED model path 时禁止使用调用者传入的 EED/growth 数值。

### P2 — 估值，不是规则门禁

`HOST_YEAR` Scout/Activation 的 capacity 应使用 observed host-year pairs，而不是只用 unique hosts；Reservoir ranking 应使用 measured novel pair EED，而不是 `max_records` 占位值。

## 本轮已修复

- `commoncrawl`、`common-crawl`、Common Crawl URL 等变体现在会被识别为排除来源。
- `reconcile_active_candidates()` 不再信任调用者把 Common Crawl 重标为 `LOCAL_DISCOVERY`。
- `network_wizards` 在 CandidateRecord reconciliation 中归入 ISC/reference-only 类别；即使进入 source discovery 图，也不能直接授权年度 evidence。
- Source discovery admission 在 triage/scout 之前拒绝 Common Crawl corpus。
- Source discovery registry 也拒绝来自 Scrapy/Scout 子图的 Common Crawl 注册。
- EvidencePlanner 即使收到错误的 `direct_year` 授权，也不会把 ISC/Network Wizards 或 Common Crawl 记录生成年度 direct capsule。

## 暂不判违规的事项

- Wayback 不稳定不改变规则分类；它只影响 EvidenceTask 的完成状态。
- Source discovery 是否由 Agent 触发不影响年度合法性，前提是最终仍经过 exact-year evidence gate。
- JISC 数据当前公共下载入口不可直接定位属于访问问题；一旦获得 CDX 文件，必须按本审计中的 direct-year 规则处理。
