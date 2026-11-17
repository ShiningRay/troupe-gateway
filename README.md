# troupe-gateway

Troupe 的**外部端点传输桥**：把"外部客户端 ↔ actor"的通用传输管理收编成开箱即用的伴生 shard，协议路由留给应用。分层边界与决策记录见 [troupe-planning ADR-0007](https://github.com/ShiningRay/troupe/blob/main/docs/adr/0007-external-endpoint-layering.md)。

## 职责边界（ADR-0007 L3）

**负责**：WS 会话生命周期、出站有界 mailbox + 限时投递 + 溢出策略（Drop/Disconnect）、Outbox 派发循环、`event_id` 去重、握手信息暴露。

**不负责**：应用协议路由、业务语义（房间/昵称）、认证授权、路由/中间件/序列化（那是 kemal/athena 的领域）。

## 快速开始

```crystal
require "troupe-gateway"

stage = Troupe::Stage.new
stage.register(RoomActor.troupe_manifest)      # 你的 actor

gateway = Troupe::Gateway::WebSocket.new(stage.store)

# 应用协议路由：上行消息 → actor 调用
gateway.on_message = ->(session : Troupe::Gateway::Session, raw : String) do
  # 解析 raw → typed agent 调用 → gateway.deliver(session, 回复)
end

# Outbox 事件路由（已按 event_id 去重；至少一次由 troupe 保证）
gateway.on_outbox = ->(record : Troupe::OutboxRecord) do
  # 按业务把 record.payload 投给相关会话：gateway.deliver_id / broadcast
end

gateway.track(Troupe::ActorId.new("chat", "room", "lobby"))  # 参与派发轮询
port = gateway.listen("127.0.0.1", 7777)
```

## API

| 成员 | 说明 |
|---|---|
| `WebSocket.new(store, config = Config.new)` | store 用于 Outbox 派发 |
| `on_message(Session, String)` | 上行原文回调 |
| `on_connect(Session) : Bool` | 接纳钩子，false 拒绝（不触发 on_disconnect） |
| `on_disconnect(Session)` | 断线清理回调 |
| `on_outbox(OutboxRecord)` | Outbox 事件回调（去重后） |
| `track / untrack(ActorId)` | 派发循环的轮询对象 |
| `deliver(session, payload) : Bool` | 限时投递；溢出按策略 |
| `deliver_id(id, payload) : Bool` | 按会话 id 投递 |
| `broadcast(payload, except id = nil) : Int32` | 全体广播（可排除），返回成功数 |
| `duplicate?(event_id) : Bool` | 事件去重查询 |
| `listen(host, port) : Int32` / `stop` | 启停（listen 返回实际端口） |
| `Session` | id / meta / path / headers / send 前的投递句柄 |

`Config`：`mailbox_cap`（默认 256）、`push_wait`（200ms）、`overflow`（Drop/Disconnect）、`dedup`（true）、`dedup_cache`（4096）、`dispatch_interval`（50ms）、`service_name`。

## 验证

```sh
crystal spec          # 路由/握手/广播/溢出/去重/断线/Outbox 派发
crystal run examples/chat_echo.cr
```

## License

MIT，见 [LICENSE](LICENSE)。
