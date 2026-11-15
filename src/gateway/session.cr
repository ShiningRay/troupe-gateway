# 会话：一条外部连接在网关内的表示。纯数据 + 出站 mailbox（容量策略由 Gateway 执行）。

require "http"
require "json"

module Troupe
  module Gateway
    class Session
      getter id : String
      getter meta : Hash(String, String)
      getter path : String
      getter headers : HTTP::Headers
      getter mailbox : Channel(String)
      property socket : HTTP::WebSocket?

      # 清理只允许执行一次（断线路径幂等）。
      @cleaned : Bool = false

      def initialize(@id : String, mailbox_cap : Int32 = 256,
                     @path : String = "", @headers : HTTP::Headers = HTTP::Headers.new)
        @meta = Hash(String, String).new
        @mailbox = Channel(String).new(mailbox_cap)
      end

      def cleaned? : Bool
        @cleaned
      end

      def mark_cleaned : Nil
        @cleaned = true
      end

      def closed_mailbox? : Bool
        @mailbox.closed?
      end
    end
  end
end
