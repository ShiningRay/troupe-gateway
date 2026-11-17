# WebSocket 网关：外部客户端 ↔ actor 的传输桥（ADR-0007 L3）。
#
# 职责（仅传输管理，协议路由归应用）：
# - 会话生命周期：注册/断线清理/on_connect 接纳；
# - 出站投递：每会话有界 mailbox + 限时 send + 溢出策略（Drop/Disconnect）；
# - Outbox 派发循环：track 的 actor id 每 dispatch_interval 轮询一次，
#   事件（至少一次）经可选 event_id 去重后交 on_outbox 由应用路由；
# - 握手信息（path/headers）暴露到 Session。
#
# 不负责：应用协议路由、业务语义、认证授权、路由/中间件。

require "http"
require "json"
require "troupe"
require "./config"
require "./session"

module Troupe
  module Gateway
    class WebSocket
      @sessions = {} of String => Session
      @tracked = {} of String => ::Troupe::ActorId
      @seen_events = Set(String).new
      @mutex = Mutex.new
      @http : HTTP::Server?
      @stopped = false
      @config : Config
      @dispatcher : ::Troupe::OutboxDispatcher

      # ---- 应用回调 ----
      property on_message : Proc(Session, String, Nil)?
      property on_disconnect : Proc(Session, Nil)?
      property on_outbox : Proc(::Troupe::OutboxRecord, Nil)?
      # 返回 false 拒绝接入（会话随即关闭，不触发 on_disconnect）。
      property on_connect : Proc(Session, Bool)?

      def initialize(@store : ::Troupe::PropStore, @config = Config.new)
        @dispatcher = ::Troupe::OutboxDispatcher.new(@store, ->(record : ::Troupe::OutboxRecord) {
          # 至少一次投递 → 事件去重后交应用路由。
          @on_outbox.try(&.call(record)) unless duplicate?(record.event_id)
        })
      end

      def config : Config
        @config
      end

      # ---- actor 注册（Outbox 派发循环的轮询对象） ----

      def track(actor_id : ::Troupe::ActorId) : Nil
        @mutex.synchronize { @tracked[actor_id.to_s] = actor_id }
      end

      def untrack(actor_id : ::Troupe::ActorId) : Nil
        @mutex.synchronize { @tracked.delete(actor_id.to_s) }
      end

      def tracked_size : Int32
        @mutex.synchronize { @tracked.size }
      end

      # ---- 会话注册（正常会话自动注册；供测试/应用造会话） ----

      def register(session : Session) : Nil
        @mutex.synchronize { @sessions[session.id] = session }
      end

      def unregister(session_id : String) : Nil
        @mutex.synchronize { @sessions.delete(session_id) }
      end

      def sessions_size : Int32
        @mutex.synchronize { @sessions.size }
      end

      # 按会话 id 查询会话（用于应用路由时读取 meta/握手信息）。
      def session(session_id : String) : Session?
        @mutex.synchronize { @sessions[session_id]? }
      end

      # ---- 启停 ----

      # 绑定并后台监听；返回实际端口（0 → 系统分配）。
      def listen(host : String, port : Int32) : Int32
        spawn(name: "gateway-outbox-dispatch") { dispatch_loop }
        ws_handler = HTTP::WebSocketHandler.new do |ws, context|
          run_session(ws, context)
        end
        server = HTTP::Server.new([ws_handler, FallbackHandler.new(@config.service_name, @config.static_dir)])
        address = server.bind_tcp(host, port)
        @http = server
        spawn(name: "gateway-http-listen") { server.listen }
        address.port
      end

      def stop : Nil
        @stopped = true
        @http.try(&.close)
      end

      # ---- 下行投递 ----

      # 投递给会话：有界 mailbox + 限时 send，超时按溢出策略处理。
      # 返回 true 表示已入队。
      def deliver(session : Session, payload : JSON::Any | String) : Bool
        raw = payload.is_a?(String) ? payload : payload.to_json
        delivered = false
        select
        when session.mailbox.send(raw)
          delivered = true
        when timeout @config.push_wait
          delivered = false
        end
        overflow(session) unless delivered
        delivered
      rescue Channel::ClosedError
        false
      end

      # 按会话 id 投递；会话不存在返回 false。
      def deliver_id(session_id : String, payload : JSON::Any | String) : Bool
        session = @mutex.synchronize { @sessions[session_id]? }
        session ? deliver(session, payload) : false
      end

      # 广播到全部会话（可排除一个 id）；返回入队成功的会话数。
      def broadcast(payload : JSON::Any | String, except session_id : String? = nil) : Int32
        ids = @mutex.synchronize { @sessions.keys }
        count = 0
        ids.each do |id|
          next if id == session_id
          count += 1 if deliver_id(id, payload)
        end
        count
      end

      # 事件去重（至少一次 → 消费侧 effectively-once；溢出整表清空）。
      def duplicate?(event_id : String) : Bool
        return false unless @config.dedup
        @mutex.synchronize do
          return true if @seen_events.includes?(event_id)
          @seen_events.add(event_id)
          @seen_events.clear if @seen_events.size > @config.dedup_cache
          false
        end
      end

      # ---- 内部 ----

      private def overflow(session : Session) : Nil
        case @config.overflow
        in Overflow::Drop
          nil
        in Overflow::Disconnect
          unregister(session.id)
          session.mark_cleaned
          session.mailbox.close
          @on_disconnect.try(&.call(session))
          session.socket.try do |ws|
            ws.close rescue nil
          end
        end
      rescue
        nil
      end

      private def run_session(ws : HTTP::WebSocket, context : HTTP::Server::Context) : Nil
        conn = Random::Secure.hex(8)
        request = context.request
        session = Session.new(conn, @config.mailbox_cap,
          request.path.dup, request.headers.dup)
        session.socket = ws

        return unless accept?(session)

        register(session)
        writer = spawn(name: "gateway-writer-#{conn}") do
          while raw = session.mailbox.receive?
            ws.send(raw)
          end
        rescue
          nil
        end

        ws.on_message { |raw| @on_message.try(&.call(session, raw)) }
        ws.run
        cleanup(session)
        session.mailbox.close
      end

      private def accept?(session : Session) : Bool
        hook = @on_connect
        return true unless hook
        hook.call(session)
      end

      private def cleanup(session : Session) : Nil
        return if session.cleaned?
        session.mark_cleaned
        unregister(session.id)
        @on_disconnect.try(&.call(session))
      end

      private def dispatch_loop : Nil
        until @stopped
          ids = @mutex.synchronize { @tracked.values }
          ids.each { |id| @dispatcher.poll_once(id) }
          sleep @config.dispatch_interval
        end
      end
    end

    # 非 WebSocket 请求的兜底应答：配置 static_dir 时尝试回静态文件，否则 404 JSON。
    class FallbackHandler
      include HTTP::Handler

      MIME = {
        ".html" => "text/html; charset=utf-8",
        ".js"   => "text/javascript; charset=utf-8",
        ".css"  => "text/css; charset=utf-8",
        ".svg"  => "image/svg+xml",
        ".png"  => "image/png",
      }

      def initialize(@service_name : String, @static_dir : String? = nil)
      end

      def call(context : HTTP::Server::Context) : Nil
        if dir = @static_dir
          path = context.request.path
          file = path == "/" ? "index.html" : path.lstrip('/')
          # 路径安全：拒绝目录穿越
          unless file.includes?("..")
            full = File.join(dir, file)
            if File.file?(full)
              context.response.status = :ok
              context.response.headers["Content-Type"] =
                MIME[File.extname(full)]? || "application/octet-stream"
              context.response.print(File.read(full))
              return
            end
          end
        end
        context.response.status = :not_found
        context.response.headers["Content-Type"] = "application/json"
        context.response.print(%({"service":#{@service_name.inspect}}))
      end
    end
  end
end
