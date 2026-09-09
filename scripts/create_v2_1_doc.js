const fs = require('fs');
const path = require('path');
const {
  AlignmentType,
  BorderStyle,
  Document,
  Footer,
  HeadingLevel,
  PageNumber,
  Packer,
  Paragraph,
  PageBreak,
  ShadingType,
  Table,
  TableCell,
  TableRow,
  TextRun,
  WidthType,
} = require('/tmp/creeper-docx/node_modules/docx');

const dataRoot = '/home/knowingthesea/Creeper-data/v3-merged260909-3';
const manifest = JSON.parse(fs.readFileSync(path.join(dataRoot, 'authority/baseline_manifest.json')));
const efficiency = JSON.parse(fs.readFileSync(path.join(dataRoot, 'reports/efficiency_v1.json')));
const performance = JSON.parse(fs.readFileSync(path.join(dataRoot, 'reports/performance_gate_v1.json')));
const pilot = efficiency.evidence_pilot;
const batchPilot = efficiency.batch_evidence_pilot;
const batchResumePilot = efficiency.batch_evidence_resume_pilot;
const candidatePilot = efficiency.candidate_pilot;
const candidatePilotCacheHit = efficiency.candidate_pilot_cache_hit;
const auxiliaryPilot = efficiency.auxiliary_pilot;
const auxiliaryEvidencePilot = efficiency.auxiliary_evidence_pilot;
const auxiliaryFullSource = efficiency.auxiliary_full_source_sample;
const auxiliaryFullEvidence = efficiency.auxiliary_full_source_evidence_pilot;
const auxiliary2001Source = efficiency.auxiliary_2001_source_sample;
const iscReferenceSample = efficiency.isc_reference_sample;
const iscEvidencePilot = efficiency.isc_evidence_pilot;
const arquivoProbe = efficiency.arquivo_cdx_probe;
const arquivoCdxjProbe = efficiency.arquivo_cdxj_probe;
const arquivoCdxjCatalogProbe = efficiency.arquivo_cdxj_catalog_probe;
const arquivoCdxjEvidencePilot = efficiency.arquivo_cdxj_evidence_pilot;
const lookup = efficiency.lookup_benchmark;
const output = '/home/knowingthesea/文档/Creeper_历史Web域名发现竞赛系统_本地主机实现_V2.1_修订方案.docx';

const blue = '1F4E78';
const lightBlue = 'D9EAF7';
const gray = 'F2F2F2';

function run(text, bold = false, color = '000000') {
  return new TextRun({ text, bold, color, font: 'Microsoft YaHei', size: 21 });
}

function paragraph(text, options = {}) {
  return new Paragraph({
    spacing: { after: 120, line: 320 },
    ...options,
    children: Array.isArray(text) ? text : [run(text)],
  });
}

function bullet(text, level = 0) {
  return new Paragraph({
    bullet: { level },
    spacing: { after: 70, line: 300 },
    children: [run(text)],
  });
}

function heading(text, level) {
  return new Paragraph({
    heading: level,
    spacing: { before: 260, after: 120 },
    children: [new TextRun({ text, bold: true, color: blue, font: 'Microsoft YaHei' })],
  });
}

function cell(text, header = false, width = 2500) {
  return new TableCell({
    width: { size: width, type: WidthType.DXA },
    shading: header ? { fill: blue, type: ShadingType.CLEAR } : undefined,
    children: [paragraph([run(String(text), header, header ? 'FFFFFF' : '000000')], { spacing: { after: 40, line: 260 } })],
  });
}

function table(headers, rows, widths) {
  const headerRow = new TableRow({ children: headers.map((h, i) => cell(h, true, widths[i])) });
  const bodyRows = rows.map(row => new TableRow({ children: row.map((value, i) => cell(value, false, widths[i])) }));
  return new Table({
    width: { size: widths.reduce((a, b) => a + b, 0), type: WidthType.DXA },
    columnWidths: widths,
    rows: [headerRow, ...bodyRows],
    borders: { insideHorizontal: { style: BorderStyle.SINGLE, size: 4, color: 'D9E2F3' }, insideVertical: { style: BorderStyle.SINGLE, size: 4, color: 'D9E2F3' }, top: { style: BorderStyle.SINGLE, size: 4, color: 'D9E2F3' }, bottom: { style: BorderStyle.SINGLE, size: 4, color: 'D9E2F3' }, left: { style: BorderStyle.SINGLE, size: 4, color: 'D9E2F3' }, right: { style: BorderStyle.SINGLE, size: 4, color: 'D9E2F3' } },
  });
}

const children = [];
children.push(new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: 160 },
  children: [new TextRun({ text: '历史 Web 域名发现竞赛系统', bold: true, color: blue, font: 'Microsoft YaHei', size: 34 })],
}));
children.push(new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: 220 },
  children: [new TextRun({ text: '本地主机实现 V2.1 修订方案与 V3 实装验证报告', bold: true, font: 'Microsoft YaHei', size: 27 })],
}));
children.push(paragraph([
  run('版本：V2.1\n', true),
  run('依据：Domain_Data_Collection_Task_0909_UpdateV3.zip\n'),
  run('基线：merged260909-3\n'),
  run('日期：2026-09-09'),
], { alignment: AlignmentType.CENTER, spacing: { after: 240, line: 360 } }));
children.push(new Paragraph({
  spacing: { after: 180 },
  children: [run('目录：请在 Word 中右键更新目录；正文标题使用 Heading 级别。', false, '666666')],
}));
children.push(new Paragraph({ children: [new PageBreak()] }));

children.push(heading('一、修订结论', HeadingLevel.HEADING_1));
children.push(paragraph('V2.1 将原设计中的“候选发现—年份证据—基线比较—EED 计算—提交导出”落地为可在 Linux x86_64 本地运行的 Python 3.12 源码。实现以 V3 的 merged260909-3 为唯一 1996–2001 年度权威基线，保留官方 hostname 规范化和 EED 计算语义，并将 Common Crawl corpus 候选发现排除在 active candidate pool 和候选评分之外。'));
children.push(paragraph('当前已经完成权威资产冻结、官方计算复现、年度位掩码索引、候选来源隔离、CDX 状态机与 HTTP 客户端、证据持久化、正式提交 precheck/exporter、资源 doctor、离线端到端演练和受限真实 Wayback pilot。'));

children.push(heading('二、V3 竞赛规则与 V2.0 兼容性修订', HeadingLevel.HEADING_1));
children.push(table(['核对项', 'V3 要求', 'V2.1 处理'], [
  ['年度权威基线', '只使用 merged260909-3/1996.txt–2001.txt', 'manifest 记录 SHA-256；辅助 deduplicated_urls_* 不进入年度索引'],
  ['hostname 身份', '精确 hostname；base、www、子域分别计数', '复现官方正则；不使用 PSL/registrable-domain 替换身份'],
  ['年份证据', '每个 hostname-year 都要有该年证据', 'year_mask 按年位 OR；CDX 按 exact host + exact year 查询'],
  ['候选来源', 'Common Crawl corpus 不得补充 active pool；ISC 独立保留', 'CandidateSourceScope 强制记录；Common Crawl 阻断，ISC 单列'],
  ['EED', '固定 CC-MAIN-2024-10 TLD 英文占比模型', '官方摘要与 breakdown 六年逐项一致，模型只用于 EED'],
  ['交付包', '六个年度 TXT、候选、说明、代码、证据和方法材料', 'formal exporter 输出根目录六年文件、源码、配置、审计和 MANIFEST'],
], [1900, 3900, 3800]));

children.push(heading('三、系统架构与源码布局', HeadingLevel.HEADING_1));
children.push(paragraph('源码位于 /home/knowingthesea/Creeper，V3 数据位于 Creeper-data 外部运行目录，避免把权威数据混入源码仓库。主要模块如下：'));
children.push(bullet('authority：官方 normalizer、EED、manifest 和可恢复 SQLite 年度索引。'));
children.push(bullet('records / sources：候选来源范围、ISC 参考适配器、静态数据集适配器和 HostObservation。'));
children.push(bullet('evidence：EvidenceCapsule、CDX 状态机、Wayback HTTP 客户端；支持 gzip、resume key、有限重试。'));
children.push(bullet('storage：SQLite evidence store，写入幂等，保留 provider、时间戳、定位、payload hash 和 policy version。'));
children.push(bullet('scheduler / metrics / runtime：有限预算、来源产出排序、资源状态机和 doctor。'));
children.push(bullet('submission：snapshot builder、precheck、正式 exporter；阻断无基线 hash、无 EED、无 CDX 审计、Common Crawl active scope 和 incomplete query。'));

children.push(heading('四、V3 权威资产与基线核验', HeadingLevel.HEADING_1));
children.push(table(['指标', '核验结果'], [
  ['年度输入总行数', '65,940,815'],
  ['1996–2001 年度去重主机名', '41,007,905'],
  ['candidate_pool 主机名', '61,507,012'],
  ['candidate 与年度基线重叠', '6,404'],
  ['基线外候选估计', '61,500,608'],
  ['source ZIP SHA-256', manifest.source_archive_hash],
  ['baseline manifest SHA-256', efficiency.baseline_manifest_sha256],
], [3500, 6100]));
children.push(paragraph('辅助 deduplicated_urls_* 文件全部被 manifest 记录，但不导入年度主表；candidate_pool_unparsed_format 也只进入未解析/复核资产。'));

children.push(heading('五、官方 EED 复现结果', HeadingLevel.HEADING_1));
children.push(paragraph('V2.1 使用官方脚本的正则和 Decimal 权重语义，逐年结果与官方 reference summary 完全一致：'));
children.push(table(['年份', 'Equivalent-English Domains'], [
  ['1996', '724,872.2116'], ['1997', '1,476,208.8181'], ['1998', '2,351,972.7407'],
  ['1999', '4,482,440.2743'], ['2000', '7,356,151.5521'], ['2001', '18,874,748.2884'],
  ['合计', '35,266,393.8852'],
], [3500, 6100]));
children.push(paragraph('Common Crawl 在此处仅作为固定 TLD primary-language share 模型来源；不得使用 Common Crawl corpus 的 URL/hostname 发现结果补充候选池。'));

children.push(heading('六、试运行与效率评估', HeadingLevel.HEADING_1));
children.push(table(['试验', '结果', '解释'], [
  ['全量基线索引', '168.712 s；峰值 RSS 24,180 KB', '批量 50,000；年度和候选分表；阶段可续跑'],
  ['续跑检查', '约 0.011 s', '七个 import_state 阶段均已完成，无需重读文件'],
  ['索引 scalar lookup', `${lookup.lookup_count} 次；约 ${Math.round(lookup.lookups_per_second)} lookup/s；p95 ${lookup.latency_ms.p95.toFixed(3)} ms`, '逐主机调用，用于保守基线'],
  ['索引 batch resolve', `${lookup.batch_lookup_seconds} s；约 ${Math.round(lookup.batch_lookups_per_second)} hosts/s`, `目标 ${lookup.batch_lookup_target_per_second} hosts/s；${lookup.batch_target_met ? '已达到' : '未达到'}`],
  ['真实 Wayback pilot', `3 条 1997 候选；${pilot.requests} 请求；${pilot.states.empty_exhaustive || 0} EMPTY_EXHAUSTIVE；${pilot.elapsed_seconds} s`, 'bounded single-worker；未把失败当负证据'],
  ['限速/恢复 pilot', `首次 ${batchPilot.executed} 执行/${batchPilot.requests} 请求；resume 跳过 ${batchResumePilot.skipped}、请求 ${batchResumePilot.requests}`, '1 req/s policy；第二次运行不重复请求'],
  ['10k 分层候选 pilot', `${candidatePilot.sample_records} 条；首次 ${candidatePilot.sampling_seconds} s`, `${candidatePilot.tld_count} 个 TLD；${candidatePilot.bucket_count} 个结构 bucket；不发网络请求`],
  ['采样缓存命中', `1k 请求 ${candidatePilotCacheHit.sampling_seconds} s`, '缓存复用；已将默认 bucket cap 固定到 32，避免 1k/10k 小幅变更触发失效'],
  ['V3 辅助 URL 百万行 pilot', `${auxiliaryPilot.raw_lines.toLocaleString()} 行；${auxiliaryPilot.elapsed_seconds} s；${auxiliaryPilot.unique_hostnames.toLocaleString()} 个唯一合法 hostname`, `年度权威重叠 ${auxiliaryPilot.annual_authority_overlap.toLocaleString()}；潜在 active ${auxiliaryPilot.potential_active_discoveries.toLocaleString()}；仍需 exact-year 证据`],
  ['辅助源 exact-year 真实 pilot', `${auxiliaryEvidencePilot.scheduled_tasks} 个任务；${auxiliaryEvidencePilot.cumulative_eed.accepted_unique_host_years} accepted；EED ${auxiliaryEvidencePilot.cumulative_eed.novel_eed_total}`, `100/100 EMPTY_EXHAUSTIVE；0 accepted；初始小 pilot 的 transient 已单独通过 resume 验证；不把失败当负证据`],
  ['1996–1997 辅助文件完整扫描', `${auxiliaryFullSource.raw_lines.toLocaleString()} 行；${auxiliaryFullSource.elapsed_seconds} s；${auxiliaryFullSource.raw_lines_per_second.toLocaleString()} 行/s`, `全文件哈希抽样 ${auxiliaryFullSource.sampled_unique_hostnames} 个，${auxiliaryFullSource.sample_potential_active} 个通过年度基线过滤`],
  ['完整文件全局样本 exact-year pilot', `${auxiliaryFullEvidence.scheduled_tasks} 个任务；${auxiliaryFullEvidence.cumulative_eed.accepted_unique_host_years} accepted；EED ${auxiliaryFullEvidence.cumulative_eed.novel_eed_total}`, `99 EMPTY + 1 transient；resume 后 100/100 终态；0 accepted`],
  ['2001–2002 辅助文件全局确认', `${auxiliary2001Source.raw_lines.toLocaleString()} 行；${auxiliary2001Source.sample_size.toLocaleString()} 个全局样本`, `5,000/5,000 与年度基线重叠；潜在 active 0；不进入网络取证队列`],
  ['ISC 参考源 exact-year pilot', `${iscReferenceSample.sampled_unique_hostnames} 个抽样；${iscEvidencePilot.scheduled_tasks} 个同年任务；${iscEvidencePilot.cumulative_eed.novel_eed_total} EED`, `97/97 最终 EMPTY_EXHAUSTIVE；0 accepted；保持 ISC_REFERENCE 隔离`],
  ['Arquivo.pt CDX 有界探针', `${arquivoProbe.raw_capture_rows} 条捕获；${arquivoProbe.unique_hostnames} 个唯一 hostname；${arquivoProbe.elapsed_seconds} s`, `publico.pt/sapo.pt；${arquivoProbe.potential_active_discoveries} 个潜在 active；只作 discovery，不直接作为年度证据`],
  ['Arquivo.pt CDXJ 有界前缀试跑', `${arquivoCdxjProbe.matching_capture_rows.toLocaleString()} 条目标年份捕获；${arquivoCdxjProbe.unique_hostnames} 个唯一 hostname；${arquivoCdxjProbe.bytes_received.toLocaleString()} bytes`, `Dinis.cdxj Range=206；${arquivoCdxjProbe.potential_active_discoveries} 个潜在 active；6 个均与基线重叠`],
  ['Arquivo.pt CDXJ 目录级探针', `${arquivoCdxjCatalogProbe.catalog_entries} 个目录条目；${arquivoCdxjCatalogProbe.selected_entries} 个小集合；${arquivoCdxjCatalogProbe.total_capture_rows.toLocaleString()} 条目标年份捕获`, `${arquivoCdxjCatalogProbe.unique_hostnames.toLocaleString()} 个唯一 hostname；${arquivoCdxjCatalogProbe.potential_active_discoveries} 个潜在 active`],
  ['Arquivo.pt 新候选 exact-year 证据', `${arquivoCdxjEvidencePilot.scheduled_tasks} 个任务；${arquivoCdxjEvidencePilot.current_run.accepted} accepted；EED ${arquivoCdxjEvidencePilot.cumulative_eed.novel_eed_total}`, 'lemac.18.lemac.ist.utl.pt / 1998；EMPTY_EXHAUSTIVE；0 capsule'],
  ['离线端到端', '5 条合成证据；precheck ready；正式包校验通过', '含六年文件、源码和 DOCX；只验证 wiring，不是竞赛成绩'],
  ['自动化测试', '48/48 通过', '包含官方 golden、CDX gzip/resume、提交合同、独立验收、采样缓存、批量解析、限速恢复、辅助源抽样/年份调度、完整文件哈希抽样、ISC 参考抽样、Arquivo.pt CDX/CDXJ/目录解析和性能换算'],
], [2600, 3200, 3800]));
children.push(paragraph('真实 pilot 当前没有新增 hostname-year，因此不产生官方 EED 增量；这只说明查询、分页、解压、状态分类和持久化链路已可运行。下一轮应做分层样本和多来源比较，而不是把 0 yield 解释为竞赛失败。'));

children.push(heading('七、基于同行实测的性能门槛', HeadingLevel.HEADING_1));
children.push(paragraph('本节把同学 C 提供的 23 天已接受结果作为未独立核验的规划参考，不把 Candidate 赛道未经规则确认地并入年度正式分数。年度正式赛道的竞争目标设为 250k EED/day，500k EED/day 为强势目标；按保守转换权重 0.56，分别需要约 446k 和 893k 个有效 raw Domain-Year/day。'));
children.push(table(['目标', '年度 EED/day', '约需 raw Domain-Year/day', '达到 C 年度速度的倍数', '估算 5% 时间'], [
  ['同档', '100,000', '178,571', '1.85×', '17.44 天'],
  ['明显领先', '250,000', '446,429', '4.61×', '6.98 天'],
  ['强势', '500,000', '892,857', '9.23×', '3.49 天'],
  ['极限规划', '1,000,000', '1,785,714', '18.45×', '1.74 天'],
], [1800, 1800, 2600, 1900, 1800]));
children.push(paragraph(`当前 batch resolve 实测约 ${Math.round(lookup.batch_lookups_per_second)} hosts/s，超过 100k hosts/s 工程门槛；但真实业务产出仍为“未证明”，因为当前 3 条 Wayback pilot 没有 accepted hostname-year。下一瓶颈是可批量枚举的高密度来源和 exact-year 证据吞吐，而不是本地索引。`));

children.push(heading('八、运行命令与产物', HeadingLevel.HEADING_1));
children.push(paragraph('核心验证命令：'));
children.push(bullet('PYTHONPATH=src python3 -m unittest discover -s tests -p \'test_*.py\' -v'));
children.push(bullet('PYTHONPATH=src python3 scripts/authority_manifest.py <task-root> <manifest.json> --source-archive <zip>'));
children.push(bullet('PYTHONPATH=src python3 scripts/build_baseline.py <task-root> <baseline.sqlite3> --batch-size 50000'));
children.push(bullet('PYTHONPATH=src python3 scripts/run_evidence_pilot.py <task-root> <index> <report-dir> --limit 3 --year 1997'));
children.push(bullet('PYTHONPATH=src python3 scripts/run_auxiliary_source_pilot.py <task-root> <index> <report.json> --total-limit 1000000 --limit-per-file 100000'));
children.push(bullet('PYTHONPATH=src python3 scripts/run_arquivo_cdx_probe.py <seeds.txt> <index> <candidates.jsonl> <report.json> --limit 25 --total-limit 50'));
children.push(bullet('PYTHONPATH=src python3 scripts/run_arquivo_cdxj_sample.py <cdxj-url> <index> <candidates.jsonl> <report.json> --max-bytes 1048576 --from-year 1996 --to-year 2001'));
children.push(bullet('PYTHONPATH=src python3 scripts/run_arquivo_cdxj_catalog_probe.py <index> <candidates.jsonl> <report.json> --max-file-bytes 20000000 --max-files 10'));
children.push(bullet('PYTHONPATH=src python3 -m creeper.cli doctor <task-root> <data-root>'));
children.push(paragraph('关键产物：baseline_manifest.json、baseline-fast.sqlite3、efficiency_v1.json、evidence-pilot-20260909/evidence_pilot.json，以及 offline-dry-run 下的正式 exporter 演练包。'));

children.push(heading('九、正式提交前检查清单', HeadingLevel.HEADING_1));
children.push(bullet('确认主办方接受根目录六个 TXT 文件及附加 code/、evidence/、reports/ 目录结构。'));
children.push(bullet('为每条新增年度记录保留 exact hostname、目标年份、证据定位、时间戳和 payload hash。'));
children.push(bullet('提交前重新计算合并后每年 EED、基线 EED、增量和增长率；不能把工程指标替代官方 p_i/S_i。'));
children.push(bullet('扩大真实来源 pilot，继续按边界接入 Arquivo.pt CDXJ 分片、UK Web Archive 等适配器，并记录来源 yield、重试、429/504、分页和 saturation。'));
children.push(bullet('保持 Common Crawl corpus 候选排除，仅保留固定 TLD 权重模型。'));

children.push(heading('十、限制与下一步', HeadingLevel.HEADING_1));
children.push(paragraph('当前实现已经能在本地主机上复现权威基线、运行完整核心流水线并生成可审计提交包，但尚未声称已完成组织者格式确认或获得任何官方竞赛增量。真实 pilot 的 3 条候选均为空，是样本结果，不是全量结论。下一阶段应在保守请求预算下扩大分层样本、接入更多历史档案源，并在正式提交前由独立校验器重新运行 manifest、EED、年度去重和证据覆盖检查。'));

const doc = new Document({
  creator: 'Creeper V2.1 local implementation',
  title: '历史 Web 域名发现竞赛系统 V2.1 修订方案与 V3 实装验证报告',
  description: 'V3 competition compatibility, implementation and verification report',
  styles: {
    default: { document: { run: { font: 'Microsoft YaHei', size: 21 }, paragraph: { spacing: { line: 320 } } } },
    paragraphStyles: [
      { id: 'Title', name: 'Title', basedOn: 'Normal', next: 'Normal', run: { font: 'Microsoft YaHei', size: 34, bold: true, color: blue }, paragraph: { alignment: AlignmentType.CENTER, spacing: { after: 180 } } },
    ],
  },
  sections: [{
    properties: { page: { margin: { top: 1000, right: 1100, bottom: 1000, left: 1100 } } },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [run('Creeper V2.1 · V3 实装验证 · 第 ', false, '666666'), new TextRun({ children: [PageNumber.CURRENT], font: 'Microsoft YaHei', color: '666666', size: 21 }), run(' 页', false, '666666')] })] }) },
    children,
  }],
});

Packer.toBuffer(doc).then(buffer => {
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, buffer);
  console.log(output);
});
