# 星载归档镜像 WAL 恢复服务 (WAL Recovery Service)

断电后只保留主数据库与可能截断的 SQLite WAL。本服务从 WAL 的**原始字节**
解析 WAL 头与帧头，核对魔数、格式版本、盐值与跨帧累计校验和，确定
**最后一个校验完整且提交数据库大小非零的帧**作为恢复前缀，只把此前各页
**最后一次出现**的页像覆盖到主库，重建最后一次可恢复提交的完整镜像。
未提交（db-size 为 0）的遥测页不会进入下行副本。`POST /recover` 也接受
可选的 `target_frame`，在任一**较早的已验证提交边界**上重建同样严格的
历史镜像，用于与已登记的遥测封存点复核。

仅使用 Python 3 标准库，无第三方依赖。

## 支持范围

- 仅小端 SQLite 3 数据库（`SQLite format 3\0` 头）与标准 WAL
- WAL 魔数 `0x377f0682`（小端校验和）；`0x377f0683`（大端）明确拒绝
- 页大小：512、1024、2048、4096（2 的幂，闭区间）
- WAL ≤ 2 MiB；请求体 ≤ 8 MiB
- WAL 格式版本 3007000

## HTTP 接口

### `GET /health`

```json
{"status": "ok", "service": "wal-recovery"}
```

### `POST /recover`

请求：

```json
{
  "database_base64": "<Base64 主数据库>",
  "wal_base64": "<Base64 WAL>",
  "page_order": "numeric",
  "target_frame": 2
}
```

`page_order`：`numeric`（按页号）或 `frame`（按各页最后出现的帧号），
默认 `numeric`。

`target_frame`：可选的 **1 基帧号**。省略或为 `null` 时恢复最后一次
可恢复提交（既有行为不变）；给出时必须指向已通过盐值与累计校验和验证、
且提交数据库大小非零的**提交帧**。引擎随后只在该帧及此前各页最后一次
出现的页像上重建镜像——后续提交与未提交事务的页像一律不进入下行副本。

成功（WAL 完整）：

```json
{
  "status": "recovered",
  "target_frame": null,
  "commit_frame": 5,
  "recovered_pages": 3,
  "wal_pages": 3,
  "db_size_pages": 3,
  "page_size": 4096,
  "valid_frames": 5,
  "wal_complete": true,
  "first_invalid_offset": null,
  "first_invalid_reason": null,
  "page_sources": [{"page": 1, "frame": 1}, {"page": 2, "frame": 5}, {"page": 3, "frame": 4}],
  "image_base64": "...",
  "digest_alg": "sha256",
  "digest": "<重建镜像 SHA-256>"
}
```

指定 `target_frame` 时，响应中的 `target_frame` 原样回显，`commit_frame`
即该历史边界；上例若 `target_frame=2`，则仅含帧 1–2 的提交内容，镜像为
2 页，页来源为 `[{"page":1,"frame":1},{"page":2,"frame":2}]`。

WAL 尾部有损坏帧但目标边界（或默认的最后提交）完全位于其之前时，仍返回
该**完整提交**的镜像，`status` 为 `recovered_with_invalid_tail`，并给出
首个失效偏移；损坏帧之后的任何页像都不会进入镜像。

目标帧无法服务时返回 `409`，**不返回任何镜像**，包括：目标帧不存在、
目标帧 db-size 为 0（属于未提交事务）、目标帧就是首个失效帧或位于其后。

失败（无完整提交、头损坏、首个帧即失效等）返回 `409`，**不返回任何镜像**：

```json
{
  "status": "unrecoverable",
  "error": "no frame carries a non-zero commit database size ...",
  "offset": null,
  "offset_scope": "wal"
}
```

非法 Base64 / 字段错误（含非正整数的 `target_frame`）返回 `400`，
WAL 超限返回 `413`。

## 失效定位规则

| 情况 | 首个失效偏移 |
|---|---|
| 魔数错误 | WAL `0` |
| 版本错误 | `4` |
| 页大小错误 | `8` |
| WAL 头校验和不符 | `24` |
| 第 n 帧盐值变化 | 帧头 `+8` |
| 第 n 帧累计校验和不符 | 帧头 `+16` |
| 非法页号（0 或超出提交大小） | 对应帧头 `+0` |
| 尾部帧截断 | 不完整帧的起始字节 |

校验顺序与 SQLite 一致：先盐值，再在“帧头前 8 字节 + 页像”上做累计
校验和，校验通过后才信任页号字段。

## 运行

```bash
# 启动服务（宿主机端口可配置，默认 8080）
HOST_PORT=9090 docker compose up --build recovery

# 一次性验收：单元测试 + 镜像构建 + HTTP 冒烟，退出码即验收结论
docker compose up --build verify
# 仅看结论：
docker compose up --build verify; echo "verdict=$?"
```

本地无 Docker 时的等价运行：

```bash
PORT=8080 python3 app/server.py
sh tests/verify.sh            # 自动起测 http://127.0.0.1:${PORT:-8080}
```

## 目录

```
app/wal_recover.py    原始字节 WAL 解析与恢复引擎
app/server.py         HTTP 服务（标准库 http.server）
tests/test_recovery.py  引擎单元测试（真实 SQLite WAL + 手工构造 WAL）
tests/http_smoke.py     HTTP 端到端冒烟
tests/walfixture.py     WAL 生成/破坏夹具
tests/verify.sh         verify 服务的一次性验收入口
Dockerfile / docker-compose.yml
```
