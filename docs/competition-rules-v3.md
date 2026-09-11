# Domain Data Collection Task V3 — 项目内强制规则

本文档把 `Domain_Data_Collection_Task_0909_UpdateV3.zip` 中的规则固化为 Creeper 的实现口径。它优先于实验脚本、source-specific 默认值和未经审查的 Agent 建议。

## 1. 正式对象与两条赛道

- 正式年度对象是精确的 `(normalized_hostname, year)`，其中 `year ∈ {1996, 1997, 1998, 1999, 2000, 2001}`。
- 年度主表 `1996.txt` … `2001.txt` 与 Candidate Pool 永远分离。
- 同一 hostname 可以出现在多个年度文件；只在同一年度内去重。
- Candidate、Discovery、DNS、未定年的目录和数据集只能作为候选或证据规划输入，不能自动进入年度主表。
- Common Crawl corpus discovery 不进入 active candidate pool；Common Crawl 的固定 TLD 模型仅可用于 EED 权重计算。

## 2. hostname 规范化

年度记录的单位是 hostname，不是 registrable domain。

- 只保留合法、规范化的 hostname；子域名分别计数。
- 路径、协议、端口、查询串、片段、用户信息、空值和明显的非 hostname 字符串不得成为记录。
- 年度文件按规范化 hostname 排序，并在年内去重。

## 3. 年度证据门槛

年度主表中的每一条 `(hostname, year)` 必须有该年份的 item-level 证据和可追溯 provenance。至少保存：

```text
target_year
evidence_type
source_file / source_id
original_url
record_locator
extraction_method
raw_hostname
canonical_hostname
evidence_timestamp 或等价的年份字段
```

### 3.1 可直接支持年度主表的证据

以下证据在年份字段与记录对象明确对应时，可以设置 `direct_year_mask`，进入正式年度 Evidence：

- Internet Archive / Wayback 的 exact-host CDX capture timestamp；
- Arquivo.pt CDX/CDXJ 的 capture timestamp；
- UK Web Archive / JISC 的年度 CDX 记录，含明确的 target-year timestamp；
- UK Web Archive host/link graph 中明确关联年份的记录；
- 带明确年份、原始 URL 和记录位置的 dated directory/index 或等价历史网页证据；
- 其他经规则审核、能把该 hostname 与该精确年份直接绑定的来源。

注意：JISC/UKWA 的年度 CDX 行属于 archive index evidence，不是普通 discovery hint。CDX 行中的 timestamp 必须保留为证据时间戳，不能只降级为 `year_hint_mask`。

### 3.2 只能用于发现或规划的证据

以下默认只能设置 `year_hint_mask` 或产生 Candidate，不能直接进入年度主表：

- ISC / Network Wizards 等 raw DNS observation；
- 未带 item-level 年份的 DMOZ、Stanford 或其他聚合目录；
- 当前网页提及、未定年链接、Usenet/邮件/README URL；
- 仅有 archive file metadata 的 `WARC-Date`，但没有经规则确认的网页记录证据；
- WHOIS creation date（只能证明“不晚于创建日存在”，不能证明后续年份持续存在）；
- 不能定位到具体记录的来源摘要或搜索结果。

这些结果必须保留在 Candidate / pending evidence，不得通过“资源来自历史库”这一点自动转入年度主表。

## 4. Evidence 与 Novelty

- Evidence 是持久事实；Novelty 相对于当前 baseline 重新计算。
- 正式过滤必须按 host-year mask 执行：

```text
need_mask = target_mask & ~(official_year_mask | local_evidence_mask)
```

- Scout 可以使用 host-only 指标作为保守先验，但带年份的资源必须优先使用 novel host-year pair 指标。
- baseline 中某 hostname 已有其他年份，不得因此删除它在目标年份的有效 pair。

## 5. EED 规则

- EED 必须按年度分别计算：每个年度文件中唯一的规范化 hostname 按固定官方模型求和。
- 同一 hostname 在两个不同年度各有合格证据时，两个年度分别计入。
- Candidate 数量、raw hostname 数量、Discovery 数量、未验证 Evidence 数量都不能直接当作年度 EED。
- 无效 hostname、未匹配 TLD 和不在正式年度文件中的记录不计入正式 EED。
- 生产报告必须同时给出 annual EED 与 Candidate 指标，不能将两条赛道未经规则确认地相加。

## 6. 提交与交付

正式提交前必须基于最新 baseline 重新生成：

1. 六个年度文件；
2. 与年度主表分离的 Candidate 文件；
3. Evidence/provenance JSONL 或等价可审计文件；
4. baseline manifest、EED calculator 输出和差异统计；
5. source contribution、CDX 执行记录、代码版本、配置、依赖和运行命令；
6. 规则变更和清洗过程说明。

未达到 exact-year evidence、normalization、provenance 或 audit 要求的结果，即使数量很大，也不得标记为 `SUBMISSION_READY`。

## 7. 运行原则

- Source discovery 不能授权年度结果；它只能产生候选、Reservoir 或 EvidenceTask。
- SourceProducer 和 EvidenceWorker 分离；Evidence provider 失败不得改变候选/年度证据分类。
- 所有长期统计必须区分 `HOST_ONLY` 与 `HOST_YEAR`，并记录 source file、record locator、adapter/parser version。
- 任何规则变更后必须重建受影响的年度结果和 EED，不得沿用旧的汇总数字。

