# Creeper V2.1 实装状态

更新时间：2026-09-09

## 已完成

- 已以 `merged260909-3` 为唯一年度基线，未把辅助文件
  `deduplicated_urls_1996-1997.txt` 混入年度主表。
- 已复现官方 hostname 正则和 EED 计算语义；1996–2001 六年摘要与官方
  参考输出逐项一致，合计 `35,266,393.8852`。
- 已建立可恢复的 SQLite 年度索引：年度出现位和官方候选集分表存储，按
  文件记录行偏移，异常中断后可以续跑。
- 已实现 V3 候选来源规则：Common Crawl corpus 排除，ISC 独立参考集，年度
  基线重叠移除，未解析字符串独立保留。
- 已实现 exact-host/exact-year CDX 状态机；不完整分页、超时和连接错误不会
  被记为 `EMPTY_EXHAUSTIVE`。
- 已接入 Wayback CDX HTTP 客户端：gzip 解压、resume-key 分页、429/5xx/网络
  错误有限重试，并完成受限真实候选 bounded pilot。
- 已加入单 worker 的 evidence batch runner：支持请求速率限制、JSONL checkpoint、
  terminal task 跳过、transient/incomplete 重试和 HTTP 请求计数；1 条真实任务
  已验证首次请求与 resume 后零重复请求。
- 已实现 SQLite evidence store，证据胶囊写入幂等并保留完整溯源字段。
- 已实现本地有限来源适配、确定性调度、有限预算和工程指标。
- 已接入 V3 `deduplicated_urls_*` 辅助 URL 适配器和有界审计：保留文件/行号溯源，
  只作为 Candidate discovery，不进入年度权威；百万行 pilot 已测得唯一合法主机、
  年度重叠、官方 Candidate 重叠和潜在 active 数量。
- 已实现辅助源的确定性 reservoir 抽样和 exact-year 证据 pilot：来源文件的两年
  时间窗只用于生成查询任务，不作为年份证据；CDX 终态、checkpoint、resume、
  accepted hostname-year 和 EED 统计均写入独立报告。
- 已加入完整辅助文件的流式哈希抽样，不再只依赖文件头部切片；可在低内存下扫描
  13M 级文件并在年度基线过滤后输出固定大小候选样本。
- 已实现带证据溯源的 submission zip 导出，并完成离线合成端到端试跑。
- 已加入 Arquivo.pt CDX 有界发现适配器和探针脚本：支持域名/主机匹配、年份范围、
  JSON 解析、行级定位和基线过滤；Arquivo.pt 记录只作为 discovery，不直接作为年度证据。
- 已加入 Arquivo.pt CDXJ 流式行解析和有限字节前缀试跑：支持 Range 请求、截断行保护、
  目标年份过滤、行号溯源和基线过滤，不会因探针而下载整套数十/数百 GB 索引。
- 已加入 Arquivo.pt CDXJ 目录级探针：自动解析官方目录、按文件大小选择有限集合、
  处理目录大小四舍五入，并把发现结果送入现有 exact-year Wayback 证据链。
- 已实现正式 snapshot precheck、六年年度文件、候选/ISC/未解析资产、EED
  报告、CDX 审计和 `MANIFEST.json` 打包。
- 已实现独立提交包验收器：校验 ZIP CRC、MANIFEST 条目哈希、基线身份、年度
  证据覆盖、候选范围和 Common Crawl 排除规则；最新离线包已通过验收。
- 已加入性能门槛模型：将年度正式赛道与 Candidate 情景分开，记录 100k/250k/
  500k/1M EED/day 档位、raw Domain-Year 需求和 ETA_5%；用户提供的同行数据
  明确标为未独立核验的规划参考。
- 已实现 `doctor` 与资源治理状态机。

## V3 全量试跑记录

数据根目录：`/home/knowingthesea/tmp/Domain_Data_Collection_Task_0909_UpdateV3/Domain_Data_Collection_Task`

运行产物：`/home/knowingthesea/Creeper-data/v3-merged260909-3`

| 指标 | 结果 |
|---|---:|
| 年度输入总行数 | 65,940,815 |
| 年度去重主机名 | 41,007,905 |
| 官方候选主机名 | 61,507,012 |
| 候选与年度重叠 | 6,404 |
| 基线外候选估计 | 61,500,608 |
| 全量索引耗时 | 168.712 秒 |
| 峰值常驻内存 | 24,180 KB |

真实 Wayback bounded pilot：当前记录为 3 条 1997 候选，3 次请求，3 次
`EMPTY_EXHAUSTIVE`，0 次 accepted，耗时约 3.87 秒；resume 再运行时 0 次请求、
跳过已完成任务；结果已写入 `reports/evidence-pilot-20260909-fixed/`。这只是
来源连通性/状态语义试验，不是竞赛得分。

索引查询 benchmark：100,000 条年度主机 + 100,000 条生成的缺失主机。逐主机
scalar 路径约 60,834 lookup/s；批量 `resolve_batch` 路径约 518,253 hosts/s，
超过计划中的 100k hosts/s 工程门槛；中位 scalar 延迟约 0.014 ms，p95 约
0.017 ms。该数字只代表当前机器、SQLite 页缓存和该样本。

分层候选 pilot：10,000 条请求完整扫描 1.3 GB candidate_pool，耗时 351.3 秒、
外部计时峰值 RSS 约 295 MB，输出 10,000 条 active 候选，覆盖 4,943 个 TLD
和 10,000 个结构 bucket。相同 10,000 条请求的缓存命中约 0.602 秒；采样器已将
默认 bucket cap 固定为 32，避免 1k/10k limit 的小幅变化使缓存失效。该结果
说明应长期复用候选采样缓存或索引，不能重复扫描原始候选文件。

V3 辅助 URL 百万行 pilot：最多每个文件 100,000 行、总计 1,000,000 行，耗时约
5.913 秒，扫描 991,551 条合法 hostname 行，去重后 707,239 个唯一 hostname；
其中 165,511 个与年度权威重叠，105,018 个已在官方 candidate_pool，541,728 个
属于“潜在 active discovery”。最后一项仍需 exact-host/exact-year 证据，不能直接
折算成 EED 或提交分数。

辅助源 exact-year 真实 pilot 分两轮：初始 10 个 `1996-1997` 来源候选生成 20 个
任务，首轮 19 个 `EMPTY_EXHAUSTIVE`、1 个 transient，resume 后 20/20 进入终态；
随后扩展到 50 个候选、100 个 exact hostname-year 任务，100/100 为
`EMPTY_EXHAUSTIVE`。两轮均为 0 accepted、0 stored capsule、EED 为 `0`。这证明了
辅助源到 exact-year 证据的批处理和恢复链路；它仍不能证明整个辅助源没有其他高产区域。

随后对完整 `deduplicated_urls_1996-1997.txt`（13,124,438 行）做全文件流式扫描，
以固定种子抽取 500 个全局唯一 hostname；500/500 不在年度基线。对其中 50 个候选
生成的 100 个 exact hostname-year 任务最终为 99 个 `EMPTY_EXHAUSTIVE` 加 1 个
transient，resume 后 100/100 终态，0 accepted、0 EED。这是当前对该来源更具代表性的
有界测量，但仍不是对整个 Wayback 或全部辅助文件的绝对否定。

对 `deduplicated_urls_2001-2002.txt` 的全文件扫描共有 1,097,867 行，其中
1,089,546 行可规范化；5,000 个全局哈希样本全部与年度基线重叠，潜在 active 为 0。
因此该来源本轮不进入网络取证队列，避免把已知基线重复消耗请求预算。

ISC 参考源完整文件抽样：1996/1997 两个文件各抽 50 个全局唯一 hostname，共 100
个样本；经年度基线过滤后保留 97 个 `ISC_REFERENCE` 记录。按其各自观测年执行
97 个 exact-year CDX 任务，首轮 95 个为空、2 个 transient；resume 后最终 97/97
`EMPTY_EXHAUSTIVE`，0 accepted、0 EED。该结果只用于参考源的同年网站证据验证，
不改变 ISC 与 active candidate pool 的隔离规则。

Arquivo.pt CDX 有界真实探针：对 `publico.pt` 和 `sapo.pt` 各取最多 25 条、覆盖
1996–2001 年，共取得 50 条捕获记录，解析出 3 个唯一 hostname；3 个均已在年度
权威基线中，潜在 active 为 0。该结果验证了外部档案接口、JSON 解析、年份保留、
来源定位和本地基线过滤链路，但不是对 Arquivo.pt 全库产出的估计。官方还提供按集合
下载的 CDXJ 索引；现有单文件达到数十至数百 GB，因此当前不直接下载，而保留后续
按集合、按分片、按流式过滤的扩展路线。

对 `Dinis.cdxj` 的有限字节试跑：Range 请求返回 `206`，完整读取该 1,519,513 字节
小集合；扫描 4,843 个完整 CDXJ 行，其中目标年份匹配 3,959 条、6 个唯一 hostname，
全部与年度基线重叠，潜在 active 为 0。该结果证明 CDXJ 解析和限量读取链路可用，但
该集合本身没有带来新增；不能据此否定其他集合。

随后对官方目录进行集合级有限扫描：目录解析出 218 个 `.cdxj` 文件，在 20 MB 文件
上限和最多 10 个文件的预算下实际选择 8 个文件，共读取约 30 MB，解析目标年份捕获
50,540 条、4,964 个唯一 hostname；其中 4,963 个与年度权威重叠，1 个潜在 active
为 `lemac.18.lemac.ist.utl.pt`（来源 `DEM-IST.cdxj`，观测年 1998）。该候选进入
Wayback exact-host/exact-year 证据链后为 `EMPTY_EXHAUSTIVE`，0 accepted、0 capsule、
0 EED。它证明了从集合级发现到证据判定的闭环，但尚未形成正式得分增量。

同行性能校准：以用户提供的同学 C 的 23 天已接受结果作为未独立核验参考，年度
EED 约 54,193/day，Candidate EED 约 405,661/day。V2.1 将 250k annual EED/day
设为明显领先目标（保守需约 446k raw Domain-Year/day），500k/day 设为强势
目标；Candidate 不在规则确认前与年度分数相加。当前真实业务产出仍未证明，因
当前 Wayback pilot 为 `EMPTY_EXHAUSTIVE`，不能据此推断全量来源产出。

限速/恢复真实 pilot：1 条 1997 候选在 1 req/s policy 下执行 1 次 HTTP 请求，
得到 1 次 `EMPTY_EXHAUSTIVE`；同一 audit 再运行时执行数为 0、HTTP 请求为 0、
跳过 1 条，证明 terminal checkpoint 不会重复请求。

测试命令：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python3 -m compileall -q src scripts
```

结果：48 个测试全部通过。离线 dry-run 产生的证据明确标记为 synthetic，
不可直接作为正式竞赛提交。最新源码包 `uv build` 已通过，wheel 和 sdist
均已做压缩/成员检查。

最新离线提交包位于：
`/home/knowingthesea/Creeper-data/v3-merged260909-3/reports/offline-dry-run/`
。本轮独立验收结果为 `ready=true`，年度证据 25 条、synthetic evidence 25 条；
这一步验证的是打包合同和流水线，不代表真实来源增量已经完成。

## 尚需正式提交前完成

1. 按主办方最终提交格式确认年度文件命名、字段和压缩包元数据。
2. 增加真实网络限速/重试配置以及来源饱和审计。
3. 完成更大、分层的真实来源增量 EED、请求成本和失败率评估；当前效率数字只代表
   本地 V3 基线索引和离线试跑。
