# Jarvis canonical 测试入口

本目录是唯一测试集合。`run_suite.py` 是内部测试 runner；不安装 CLI、不修改产品、不启动模型任务，不替代独立验收。保留既有测试方法；rev9 按已复现缺陷和当前公开契约迁移必要 fixtures，入口仍直接引用实际文件和方法。

## 目标与运行

本包目标为 **rev9 / 0.3.0 候选元数据**，不是正式发布认可。Windows 产品 `code_sha256` 为 `621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226`（runner 默认）；Linux 为 `016a38db534153a5d674b89b112af5e846963da59cc4a4cf0cb0221f37b32b12`。两者仅因身份映射的路径分隔符不同。命令显式传递平台 pin；运行时同时检查 pyproject、实际加载版本、产品源码哈希及加载路径。失败输出回执并返回非零。

在解压后的 review 根目录（直接包含 `source/`、`skills/`）执行；使用声明依赖已安装的独立 Python >=3.11 venv：

```powershell
$testSource = (Resolve-Path 'source').Path
$testRun = Join-Path $env:TEMP ('jarvis-core-' + [guid]::NewGuid().ToString('N'))
$testPython = '<TEST_VENV>/Scripts/python.exe'
& $testPython -B source/tests/run_suite.py --source $testSource --expected-version 0.3.0 --expected-code-sha256 621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226 --receipt-dir $testRun --profile core --deadline 90
Get-Content (Join-Path $testRun 'report.md')
```

Linux 在同一解压根目录运行（每次换新 receipt 路径）：

```sh
<TEST_VENV>/bin/python -B source/tests/run_suite.py --source "$PWD/source" --expected-version 0.3.0 --expected-code-sha256 016a38db534153a5d674b89b112af5e846963da59cc4a4cf0cb0221f37b32b12 --receipt-dir /tmp/jarvis-rev9-core-fresh --profile core --deadline 90
```

每次使用全新的 `--receipt-dir`，必须位于 source 之外。依赖声明在 `source/pyproject.toml`，安装方法见包内 `CLOUD_COLD_START_GUIDE_REV6.md`；入口不自动安装依赖。本轮 Windows 检查环境为 Python 3.14.5、MCP 2.3.0。SDK 2.3 使用两条 HTTP transport 流、秒数 timeout 和 `httpx2`；本地客户端 `trust_env=False`。其他 SDK 版本需要先核对真实契约。

| Profile | 范围 | 用途 |
|---|---|---|
| `core`（默认） | 17 个选定既有方法 | 八板块代表性核心回归，包含直接相关合同/兼容检查 |
| `entry` | 7 个新入口 E2E | 发现、身份、失败统计、缺口、超时、取消、子进程回收 |
| `protocol` | 3 个新 HTTP/SDK E2E | 共享客户端、schema 隔离/重放/冲突、重连、未知 close、资源记录 |
| `resources` | 4 个直接相关方法 | 短时有界资源复测、stdio 超时、流式调度、分页 GC |
| `all` | 全部发现的 source/legacy/suite 方法 | 显式完整运行；保留原失败、平台 skip 和授权阻塞 |
| `acceptance` | `all` 加 8 个真实证据缺口 | 展示未运行及未实现项；不会触发真实模型或生产 |

通常只运行受影响 profile / selector。不要默认重跑 `all`。本次保留先前完整 baseline，未把它重新算作新入口成绩。

```powershell
# 只发现，不执行：也会在独立 worker 中校验加载的源码。
& $testPython -B source/tests/run_suite.py --source $testSource --expected-version 0.3.0 --expected-code-sha256 621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226 --receipt-dir ($testRun + '-list') --profile all --block B05 --list

# 单个已发现 selector（支持重复 --select；精确匹配，不猜名称）。
& $testPython -B source/tests/run_suite.py --source $testSource --expected-version 0.3.0 --expected-code-sha256 621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226 --receipt-dir ($testRun + '-one') --select 'suite::test_suite_protocol_e2e.SharedHttpE2E.test_unknown_close_is_durable_and_never_confirms_release' --deadline 30

# 查看真实证据缺口，不发起模型请求。
& $testPython -B source/tests/run_suite.py --source $testSource --expected-version 0.3.0 --expected-code-sha256 621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226 --receipt-dir ($testRun + '-gap') --select 'gap::MODEL_SCHEMA_TERMINAL'
```

`--block` 可重复，按所选 profile 的交集筛选。`coverage_manifest.json` 映射 B01 功能与接口、B02 交付正确性、B03 生命周期与退出、B04 资源使用与回收、B05 并发与隔离、B06 故障与恢复、B07 持久化与版本兼容、B08 安全与可追溯。板块方法数有重叠，不能相加成独立总数。每个 case 记录稳定 ID、实际 selector、文件/行号/哈希、断言来源、依据、前置条件、证据级别、影响及清理合同。

## 回执与退出

每次生成 `receipt.json`、`report.md`、`plan.json`、`progress.json`、`worker.log` 和 case artifacts。回执含真实加载身份、测试输入哈希、方法状态与 subTest 事件、板块计数、进程资源样本、清理读回及 artifact SHA256。结构见 `run_receipt.schema.json`。

新入口 E2E 的嵌套回执和新 HTTP E2E 的临时配置/状态均保存在本轮 case-artifacts 中，进程退出后保留供复核。未知 close 按既有 `ManagementCancellation` 与 adapter 合同允许增加 `stop_requested`，原输入字段和 ack 必须保持，终止/释放标志不得虚构；首轮新测试的“request 字节不可变”错误断言修正有据可查，原失败回执保留。

| Exit | 含义 |
|---|---|
| 0 | `listed` 或本次选定范围全部通过且清理确认 |
| 1 | 方法失败/error 或 worker 非正常退出 |
| 2 | skipped / partial / not_run / not_implemented、空选择或未确认清理 |
| 3 | 来源拒绝或 runner 错误 |
| 124 | 全运行 deadline，保存中间报告并清理专属进程树 |
| 130 | Ctrl-C 或 `--cancel-file` 已出现，保存报告并清理 |

`methods_run` 只计实际进入 unittest 方法的次数；导入错误单列 error，不能冒充已运行方法。方法失败数与 failure records、方法 error 数与 error records 分开。整方法 skip 与 subTest skip 分开；含跳过子项的方法为 `partial`，不能算全通过。合成 `fixture::` 专门测试这些结果，默认 profiles 不执行故意失败的 fixtures。

取消文件路径由调用方指定；创建该文件只取消本轮。Windows 启动器以 suspended 状态创建，进入新建 kill-on-close Job 后才恢复，其 venv 解释器与子进程继承归属；gate 验证真实 worker PID 后才执行测试。只关闭本轮 Job，不按名称结束进程。Linux 采用独立 process group；rev9 各平台的实际执行结果以新回执为准，不能沿用 rev8 Linux 成绩。嵌套 Job 的约束依据 [Microsoft AssignProcessToJobObject](https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject)。

资源记录包含测试 Job 内 PID、创建时间、CPU、RSS/private/peak RSS、句柄及退出残留。新 HTTP service 使用临时空 Home、临时 state/DB、新 loopback 端口，禁用 live creation、desktop recovery、通知与 Host 调度。资源数值是测量结果；没有性能阈值，不宣称达标或无泄漏。短时回收成功也不等于 99 席容量验收。

## 证据边界

方法数和 selector 数以本包 runner 的 `--list` 回执 `catalog` 为准，发现数不是通过数。`bundled_legacy_files` 保留根仓库旧测试的来源标记，这些文件实际已在本包同一个 `source/tests/` 中；执行时产品模块必须来自显式 rev9 source。suite E2E 只发现一次。一个真实模型网络 case 固定阻塞，不能把旧 root 或 rev7 分数算作本轮成绩。

既有 mocks/fakes、isolated 合同及迁移测试保留，但只能证明其边界。新共享 HTTP 是实际 TEST 服务与实际 MCP SDK；未知 close 的 thread/turn/ack 是明示合成状态，没有真实 Host，不能证明权威 recovery。

rev7 历史 baseline 为 385 方法、3 failure、2 error；其原回执保存在维护仓库，未附于本包，也不是本轮运行结果。包内 `REV8_REPAIR_REPORT.md` 明确标为 rev8 历史修复证据；rev9 的变更和本轮回执索引见 `REV9_DELIVERY_REPAIR_REPORT.md`。本轮累计迁移版本/公开契约及复现的 legacy fixtures，保留安全行为断言；新增内存用例先红后代码，范围与先红回执见后续 A/B handoff。真实模型→终态→release、原代权威 recovery、99 席/20+30 容量、资源验收阈值、整机 admission 的独立/真实负载证据（非持续硬限制）、Loop pause/resume、listener 故障重建、可选本地集成依赖仍分别保留为 manifest 缺口。

验收 profile 返回 incomplete/failed 是有效结果；不能通过删除 case、修改产品期望或把 not_run 改为 passed 来生成全绿。`protected_effects` 是本入口授权范围及 fixtures 约束的声明，源码/既有文件是否保留由交付 readback 另行验证；它不是对全机生产状态的审计。
