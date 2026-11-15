# Troupe Gateway 配置（ADR-0007 L3：外部端点传输桥）。

require "json"

module Troupe
  module Gateway
    # mailbox 溢出策略：
    # - Drop：丢弃该条消息，会话保留（在线尽力，默认）；
    # - Disconnect：丢弃并断开会话（防慢消费者长期占用）。
    enum Overflow
      Drop
      Disconnect
    end

    record Config,
      mailbox_cap : Int32 = 256,
      push_wait : ::Time::Span = 200.milliseconds,
      overflow : Overflow = Overflow::Drop,
      dedup : Bool = true,
      dedup_cache : Int32 = 4096,
      dispatch_interval : ::Time::Span = 50.milliseconds,
      service_name : String = "troupe-gateway"
  end
end
