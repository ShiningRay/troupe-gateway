require "spec"
require "http"
require "../src/troupe-gateway"

# Crystal 1.22 的 Channel 没有非阻塞 try_send/try_receive：select + timeout 实现。
def try_send_nb(channel : Channel(T), value : T, wait = 250.milliseconds) forall T
  sent = false
  select
  when channel.send(value)
    sent = true
  when timeout wait
    sent = false
  end
  sent
rescue Channel::ClosedError
  false
end

def try_receive_nb(channel : Channel(T), wait = 250.milliseconds) forall T
  msg = nil
  select
  when m = channel.receive
    msg = m
  when timeout wait
    msg = nil
  end
  msg
rescue Channel::ClosedError
  nil
end

# 起一个网关（内存 Stage + PropStore，供 Outbox 派发用），返回 {gateway, stage, port}。
def new_gateway(config = Troupe::Gateway::Config.new)
  stage = Troupe::Stage.new
  store = stage.store
  gateway = Troupe::Gateway::WebSocket.new(store, config)
  port = gateway.listen("127.0.0.1", 0)
  {gateway, stage, port}
end

# 测试用 WS 客户端：入站消息进 Channel。
class TestClient
  getter inbox = Channel(JSON::Any).new(128)
  @ws : HTTP::WebSocket
  @sendq = Channel(String).new(64)

  def initialize(url : String, headers : HTTP::Headers = HTTP::Headers.new)
    @ws = HTTP::WebSocket.new(URI.parse(url), headers)
    @ws.on_message do |raw|
      @inbox.send(JSON.parse(raw))
    rescue
      nil
    end
    spawn(name: "test-writer") do
      while raw = @sendq.receive?
        @ws.send(raw)
      end
    rescue
      nil
    end
    spawn(name: "test-reader") do
      @ws.run
      try_send_nb(@inbox, JSON.parse(%({"type":"disconnected"})))
    rescue
      nil
    end
  end

  def send_raw(raw : String) : Bool
    try_send_nb(@sendq, raw)
  end

  def close : Nil
    @ws.close rescue nil
    @sendq.close
  end

  def next_message(timeout = 3.seconds) : JSON::Any?
    msg = nil
    select
    when m = @inbox.receive
      msg = m
    when timeout timeout
      return nil
    end
    msg
  end

  # 等待首条满足条件的消息（跳过其余），超时抛错。
  def expect(timeout = 3.seconds, &match : JSON::Any -> Bool) : JSON::Any
    deadline = Time.instant + timeout
    loop do
      remaining = deadline - Time.instant
      raise "等待消息超时" if remaining <= 0.seconds
      msg = next_message(remaining)
      next unless msg
      return msg if match.call(msg)
    end
  end
end
