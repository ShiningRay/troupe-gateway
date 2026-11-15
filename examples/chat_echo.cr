# 最小示例：echo 网关 —— on_message 原样回发；track 一个 emitter actor
# 演示 Outbox 派发循环。运行：
#
#   crystal run examples/chat_echo.cr
#
# 另开终端用任意 WS 客户端连 ws://127.0.0.1:7790/ 发文本即回显；
# GET http://127.0.0.1:7790/ 返回 404 JSON（兜底应答）。

require "../src/troupe-gateway"

class EchoProps
  include JSON::Serializable
  property n : Int32 = 0

  def initialize
  end
end

class EchoActor < Troupe::Actor(EchoProps)
  troupe_manifest "echo", schema_version: 1 do
    turn beep, {} of String => String, returns: Int32
  end

  def beep : Int32
    props.n += 1
    emit_event("beep", JSON.parse(%({"n":#{props.n}})))
    save
    props.n
  end
end

stage = Troupe::Stage.new
stage.register(EchoActor.troupe_manifest)

gateway = Troupe::Gateway::WebSocket.new(stage.store)
gateway.on_message = ->(session : Troupe::Gateway::Session, raw : String) do
  gateway.deliver(session, %({"type":"echo","text":#{raw.inspect}}))
end
gateway.on_outbox = ->(record : Troupe::OutboxRecord) do
  gateway.broadcast(record.payload.to_s)
end

echo_id = Troupe::ActorId.new("demo", "echo", "1")
gateway.track(echo_id)
EchoActor::Agent.new(stage).beep(echo_id)

port = gateway.listen("127.0.0.1", 7790)
puts "troupe-gateway echo: ws://127.0.0.1:#{port}/"
sleep
