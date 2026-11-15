# troupe-gateway：Troupe 外部端点传输桥（ADR-0007 L3）。
#
#     require "troupe-gateway"
#
#     gw = Troupe::Gateway::WebSocket.new(store,
#       Troupe::Gateway::Config.new(mailbox_cap: 256, overflow: Troupe::Gateway::Overflow::Drop))
#     gw.on_message { |session, raw| ... }   # 应用协议路由
#     gw.on_outbox  { |record| ... }         # Outbox 事件路由（已按 event_id 去重）
#     gw.track(actor_id)                     # 参与派发轮询的 actor
#     port = gw.listen("127.0.0.1", 7777)
#
# 职责边界见 troupe-planning ADR-0007：会话生命周期、有界 mailbox 与溢出策略、
# Outbox 派发循环、事件去重。协议路由/业务语义/认证授权归应用。

require "./gateway/config"
require "./gateway/session"
require "./gateway/web_socket"

module Troupe
  module Gateway
    VERSION = "0.1.0"
  end
end
