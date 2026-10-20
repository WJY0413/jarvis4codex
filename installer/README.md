# Jarvis 离线安装与自动配置

默认目标是“配置为可创建普通任务”，不是默认停在验收模式。安装本身不创建业务任务、不运行模型、不启动或重启 Codex、App Server、后台服务。

## 最简安装

在已经有合法 Jarvis 执行上下文的目录中运行：

    ./install.sh --target /absolute/new/jarvis-version-directory --apply

安装器优先自动选择现有兼容 CPython 3.12，再选择当前包自带的 Python（如有）。不会下载 Python，不往已有 Python 环境写包。也可明确使用 --python /path/to/python3.12 或 --mode bundled。

上下文仅来自一个明确的非秘密 launcher 配置：

1. 指定 --context-config /absolute/path/to/launcher.json；或
2. JARVIS_LAUNCHER_CONFIG 环境变量；或
3. 当前调用目录内 runtime/config/launcher.json 或 config/launcher.json。

隐式来源超过一个时不会猜测；请明确选一个。不会递归扫描 HOME、凭据目录或磁盘，也不会把“当前目录存在”当作项目授权。

## 默认自动配置的条件

- 保持现有已授权项目集合，不自行增加项目。
- 只引用原有 CODEX_HOME，不读取、复制或创建认证文件。
- 现有 CLI 必须与当前所选环境里的 codex 一致，固定路径、实体和文件 hash，在状态探测前后复核。版本输出仅识别兼容协议，不证明发行者签名；CLI 的信任来自已选定的既有环境/上下文。
- 身份状态只通过该已有 CLI 的 --version 和 login status 检查。CLI 自己可能正常使用已有认证；安装器不读取认证内容，不修改登录，不保留原始输出，不传递 API key 环境变量。
- 既有执行约束须明确为 OpenAI、read-only、approval-policy=never、无额外环境。
- 预算/隔离策略路径与 scheduling_generation_id 原样保留并验证，绝不通过旧配置覆盖或丢失这些约束。
- 普通 stdio 模式不要求 Unix owner pin。只有用户已明确选择 owner-control 模式时才校验原有 pin；不捕获新 pin，不连接或启动 owner。
- 用户主动禁用的 live=false 保留。只有带匹配配置 hash、明确标注安装器 isolated 默认的来源记录才可迁移；无标记旧配置或内容冲突会明确阻塞，不把暂停猜成模板默认。

配置和策略在计划后变化、认证不明、项目缺失等情况下，代码可以安装完成，但结果为 installed_but_not_ready、退出码 3，runtime/readiness.json 列出具体缺项。不会悄悄写 live=false 再报告“已可用”。失败目标保留用于诊断；此版本仅支持全新独立目标，不会覆盖现有目标。修复上下文后使用另一个新目标重新安装。

## 明确区分三个结果

- installation_ready：文件、依赖和应用身份安装通过。
- configuration_ready / task_creation_enabled：普通任务入口配置完整，并核验了既有身份状态；总体状态为 configured_for_task_creation。
- execution_verification=not_run、live_execution_verified=false、task_started=false：安装没有派发真实任务。因此 execution_ready 为 null（未做真实执行验收），不等于禁止创建，也不能理解为模型任务已经成功运行。

生成的 bin/jarvis-mcp 已自动指向本安装独立 runtime/config 与 runtime/state，不需要手工拼接配置参数。它不会在安装过程中自动启动。第一次业务任务仍受用户授权、运行时约束和实际服务状态限制。

## 显式隔离验收

    ./install.sh --profile isolated --target /absolute/new/isolated-test --sdk-smoke --apply

该模式明确生成独立合成 HOME/项目和 live=false，结果为 installed_isolated，绝不报告任务创建已启用。可选 SDK smoke 只启动有界临时 stdio MCP 子进程，检查 19 个工具、能力与 preflight，零模型、零业务任务，测试结束关闭进程。它不会启动 Codex App Server。

## 支持范围与回退

本包仅支持 Linux x86_64、CPython 3.12、glibc >= 2.34；实测主机为 glibc 2.41，不支持 musl/Alpine。两种 Python 版本共用同一个 Jarvis wheel 与精确锁定依赖，不复制现有 venv。

安装只写新的目标目录。原 Python、原 Jarvis 配置、正式版、认证、权限和服务保持不变。配置以独立临时目录验证后原子发布；未通过时不生成可调用的默认配置。回退仍选择原来的独立安装目录，不自动更改任何现有实例指针。

## 后续版本默认设置（capacity99-v1）

普通执行配置缺省为 worker_capacity=99、live_creation_enabled=true。99 是用户可修改的总容量设置，不代表平台提供 99 个席位，也不表示安装会启动 99 个任务。实际执行仍受 provider 限流、内存保护和既有两席调度策略约束，本次不修改这些约束。

已有明确的自定义 worker_capacity 保留；只有来源记录与配置 hash 匹配、确认该值属于安装器自动默认且未被用户自定义时，才自动迁移旧默认到 99。无来源证明的显式旧数字不猜测。用户明确暂停 live=false 保留；合法上下文省略该字段时使用新的 true 默认。认证、授权项目或策略缺失仍明确 blocked，不以默认 true 假称可执行。隔离验收模式仍明确 live=false，安装不派发任何真实任务。
