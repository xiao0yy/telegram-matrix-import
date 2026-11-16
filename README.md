# Telegram Matrix Import

独立 Go 程序，将 Telegram 单聊（包括机器人）、普通群和非论坛超级群，按消息 ID 从旧到新导入**新的加密 Matrix 房间**，再交给现有 mautrix-telegram 桥接续。

当前适配 **Tuwunel 2.0.0 的原生 S3 媒体布局**、**mautrix-telegram v0.2610.0 / mautrix v0.32.0 数据库**。这里的 S3 直传是针对这些版本的适配，并非通用 Matrix 上传接口。不要直接套用于其它 homeserver、媒体多副本配置或未知桥版本。

项目仍在开发和联调中，尚未发布稳定版本。导入器独立于 Matrix Admin；后者不是运行依赖。

## 获取程序

GitHub 仓库：[xiao0yy/telegram-matrix-import](https://github.com/xiao0yy/telegram-matrix-import)。

推送 `main`、提交 Pull Request 或手动运行 **Build and test** 工作流，会执行单元测试、静态检查以及隔离的 Tuwunel/S3/加密房间测试。全部通过后，在该次 Actions 运行的 **Artifacts** 中下载 `tg-import-linux-amd64-<commit>`。

解压下载的 artifact ZIP，再校验和解包：

```sh
sha256sum -c SHA256SUMS
mkdir tg-import
tar --no-same-owner -xzf tg-import-linux-amd64.tar.gz -C tg-import
./tg-import/tg-import version
./tg-import/tg-import -help
```

运行包包含 Linux amd64 静态程序、示例配置、使用说明及依赖许可说明。程序使用 musl 构建，不依赖构建机器的 glibc 版本；运行机器需要系统 CA 证书和 Docker CLI。真实配置、会话和导入状态不进入构建产物。

## 数据流

1. 读取一页 Telegram 消息，将消息、文件身份和分页游标事务写入 SQLite。
2. 按顺序处理一条消息。附件先预留 MXC，向 Tuwunel 登记 1 字节加密文件的媒体记录。
3. Telegram 下载流经过 AES-CTR 加密，以默认 8 MiB 缓冲直接写入同一 S3 key 的 multipart upload。每个确认上传的分片、偏移、加密参数和 SHA-256 状态落盘。
4. 完成 S3 上传后，持久化包含真实文件大小、类型、密钥及哈希的加密 Matrix 事件，再使用固定事务 ID 发送。附件无需完整下载到本地，也不会通过 Tuwunel 上传整个文件。
5. 单条完成后继续下一条。收到的事件 ID 落盘，重启会接着未完成的消息处理。
6. 完成后，将 portal、消息 ID 映射和该房间的历史解密密钥交给桥。桥的 Telegram 登录、更新游标、Matrix 设备账户与发送会话不替换。

媒体始终通过原 MXC 使用 Tuwunel 的原生访问控制。客户端看到的加密事件只会引用上传完成的文件。没有额外媒体服务。

## 构建和测试

源码构建需要 Go 1.27.3、C 编译器（SQLite）、Linux；集成测试还需要 Docker 和 Python 3.9+。`goolm` 提供纯 Go Matrix 加密，不需要 libolm。首次编译 Telegram 生成代码需要较多内存，建议交给 GitHub Actions；构建缓存会在后续运行复用。这与运行时附件缓冲大小无关。

```sh
make build
make test
# Docker 中的空白 Tuwunel + 内存 S3；只绑定 127.0.0.1。
make integration
# 生成便于分发的静态包；另外需要 musl-tools、binutils、Python 3.9+。
make dist VERSION=dev COMMIT="$(git rev-parse HEAD)"
```

隔离测试不使用生产配置。测试进程结束时移除自己的容器和临时数据。测试覆盖：真实 MXC 读取 S3 直传内容、接收端解密、原始时间戳、发送去重、历史密钥交接、桥 SDK 读取导入映射，以及断点恢复和故障清理。

## 配置与运行

将 `config.example.json` 复制到仓库之外的私密路径，填入本机实际配置并设为 `0600`。`state` 使用独立的绝对路径，权限必须为 `0700`。每个会话使用一个状态目录；其中含登录凭据、消息记录、文件密钥和加密数据库，不要提交到 Git。

`matrix.as_token` 来自桥的 appservice；`admin_token` 是有媒体删除权限的管理员凭据，若使用 double-puppet appservice token，`admin_user` 填管理员 MXID。若 Tuwunel 的 `ip_source` 要求代理请求头，设置 `matrix.client_ip_header` 为对应头名（例如 `X-Real-IP`）；本地请求使用 `127.0.0.1`，包括加密设备登录。未配置 `ip_source` 时可省略此项。S3 配置必须与 Tuwunel 当前使用的单一媒体后端及前缀一致，并支持对象版本、multipart、列举与删除指定版本。

单聊可填 `peer.username`。群聊先用 `dialogs` 找到 `kind` 和 `id`；论坛群与广播频道当前不支持。每次连接都会重新解析本账号的 access hash，不从其它账号复制。

```sh
bin/tg-import -config /private/import.json login
bin/tg-import -config /private/import.json dialogs
```

独立登录会在终端隐藏输入手机号、验证码及两步验证密码。也可以在**原桥所在的同一台服务器**设置 `borrow_bridge_session: true`：停桥后使用现有登录的私密副本，不需要再次登录。此模式全程要求桥停止，不能和桥同时运行，也不能将副本复制到其它服务器并行使用；程序不会改写原桥登录或更新游标。

开始导入前，进入现有部署系统的维护状态，防止控制器、计划任务自动启动桥或修改数据库。程序不擅自停止或恢复你的服务；每条消息前和交接前都会检查桥容器已停止、自动重启策略为 `no`、镜像仍与配置一致。Tuwunel 的本地 HTTP 接口必须保持可用。

```sh
bin/tg-import -config /private/import.json inspect
bin/tg-import -config /private/import.json create-room
# 接受新房间邀请，确保自己的 Matrix 设备已在房间中。
bin/tg-import -config /private/import.json run
```

`inspect` 保存真实会话身份，只显示会话标识和消息范围，不打印聊天正文或 access hash。`create-room` 可重复执行；它识别自己的房间标记，不重复创建。`run` 拒绝导入到不属于该任务的旧房间。消息发送使用原 Telegram 时间戳；Tuwunel 中的实际插入顺序由逐条导入保证。

另一个终端可随时查看进度：

```sh
bin/tg-import -config /private/import.json status
```

`messages_discovered` 是已分页读取的数量，**不是会话总数**。`messages_sent` 是已确认发送的数量；`uploaded_bytes` 是当前记录中已确认上传的分片字节数，包含尚待清理的失败附件；`attachments_failed` 和 `cleanup_pending` 分别显示失败附件与待清理任务数。运行输出每条消息与分片的状态，不用消息 ID 比值估算百分比。

## 重试、退出和清理

- `Ctrl-C` 只停止本程序。SQLite、multipart 上传和已有消息保留用于续导；再次执行 `run` 即可。不能在待续导期间先启动桥写入同一房间。
- 单个 Telegram 附件默认最多尝试 **5 次（含首次）**。仍失败时，保留正文/说明、文件名，并在原位置发送“附件导入失败”，继续下一条；后续不会补回或替换这条附件。配置 `attachments.max_attempts` 可调整上限。
- 默认连续 **3 个附件**失败后停止，配置 `attachments.max_consecutive_failures` 可调整。成功发送一个附件清零连续计数，文字消息不清零。计数和消息发送结果一起写入 SQLite，进程重启不会漏计、重复计数或解除停止状态。阈值附件的失败提示会先发送，下一条消息保持未处理。
- Telegram 单次 RPC 最长等待 2 分钟，包括独立下载连接；超时后关闭当前下载连接并从已确认分片重试，不限制整个大文件的总传输时间。附件下载超时也计入该附件的尝试上限；取消整个任务不算一次失败。
- Telegram 明确返回限流时，按服务器要求等待，日志显示等待秒数和恢复重试；这段等待不占单次 RPC 的超时预算。进度输出包含时间、当前消息和附件尝试次数，进程存活不代表仍有进展。
- `run`、`catch-up`、`cleanup` 和 `handoff` 自动处理网络断线、HTTP 408/429/5xx、Telegram 临时服务错误。等待 30 秒后重新连接，从原断点继续；连续失败按 30/60/120/240 秒退避，最多每 5 分钟尝试一次，服务要求等待更久时遵守服务端时间。等待期间仍持有任务锁，不能启动第二个导入器。
- S3 写入、Matrix 请求、Telegram 登录/权限和限流错误独立处理，不消耗附件的失败预算。临时基础服务故障保留当前附件和时间线断点，按上述退避自动重试；附件之间的连续失败计数保持不变。
- 配置不一致、401/403、权限或数据库错误等不自动重试，`status` 显示 `execution.state=blocked` 及原因；修复后再运行原命令。达到连续附件失败上限也进入 `blocked`，不会自动重启绕过；`status` 的 `attachment_failures` 显示连续计数、停止状态、最近附件错误和消息 ID，`attachment_policy` 显示配置上限。自动等待时显示 `retry_wait`、`next_retry_at` 和上次错误。取消命令显示 `stopped`，不会自动复活。
- 已确认分片能继续使用时从最后一个确认偏移下载；远端 multipart 过期或分片缺失时，清理该上传并重写当前附件。文件引用过期由 Telegram SDK 刷新，并核对原文件身份。
- Matrix 发送响应丢失时，持久化的密文与固定事务 ID 保证重试不产生第二条消息。
- 失败附件的媒体记录、同 key 对象版本和 multipart，以及成功附件的旧 1 字节版本，均有持久化清理记录。不会对整个 bucket 或其它媒体执行删除。正常结束会执行清理；临时失败自动重试，权限等永久错误返回非零，修复后运行 `cleanup`。
- 暂停中的上传不属于失败清理队列，仍归任务状态管理。不要手动删除状态目录后期待程序找回这些对象。

达到连续失败上限后，检查并处理下载故障，再明确解除停止状态：

```sh
bin/tg-import -config /private/import.json reset-attachment-failures
# 桥仍需保持停止，继续原来的 run 或 catch-up 命令。
bin/tg-import -config /private/import.json run
```

重置命令需要独占任务锁，运行中不能重置；它只清零连续失败计数，保留上次停止记录、已发送的失败提示、分片与消息断点。提高配置阈值或仅重启进程不会解除已经触发的停止。由维护服务托管时，重置后重启该维护服务，不要另开一个并发导入器。

```sh
bin/tg-import -config /private/import.json cleanup
```

程序内部的自动重试不等于进程托管。需要防止进程崩溃或服务器重启中断时，应把**现有维护、导入、交接和恢复服务的完整流程**交给 systemd 等服务管理器：异常终止后重启、退出码 1 不循环重启、退出码 75 表示可重试的临时失败。`systemctl stop` 应停止任务，成功交接后不应重新导入。不要只自动重启借用了桥登录的导入进程，却让桥同时自动启动。

## 交给桥

桥继续保持停止。先导入开始后出现的新消息，再提交桥映射：

```sh
bin/tg-import -config /private/import.json catch-up
bin/tg-import -config /private/import.json handoff
```

交接要求所有已发现消息均完成、清理队列为空。SQLite 在线快照保留在私密 `state/bridge-backups` 中。该会话若已映射到其它 Matrix 房间，会拒绝交接；不会覆盖已有 portal。数据库提交后重复执行也不会重复插入消息。

交接成功后恢复原桥、控制器和计划任务。新消息由桥处理，导入任务关闭，不能再用 `run` 向旧时间线追加历史。应实际检查新消息双向投递、附件读取和回复，不仅检查容器是否运行。

当前保留普通文本、原始发送者、时间、回复关系，以及照片、视频、音频和文件。相册按原 Telegram 消息分别导入；不恢复原编辑/删除历史、反应、交互式按钮、投票或自定义表情效果。无法支持的消息类型会留文字说明。

## 许可

本项目代码采用 MIT 许可。第三方依赖保留各自许可；CI 运行包中的 `THIRD_PARTY.md` 和 `licenses/` 提供依赖版本、对应源码下载地址与许可文本。
