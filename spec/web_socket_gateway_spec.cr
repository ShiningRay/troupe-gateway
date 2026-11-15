require "./spec_helper"

# 传输桥语义：路由回调、会话元数据、投递/广播、溢出策略、断线清理。
module Troupe
  module Gateway
    describe WebSocket do
      it "on_message 收到上行原文与握手信息，应用可回发" do
        gateway, stage, port = new_gateway
        got = Channel(Tuple(String, String, String, String)).new(4) # id, path, header, raw

        gateway.on_message = ->(session : Session, raw : String) do
          ua = session.headers["X-Test"]?
          got.send({session.id, session.path, ua || "", raw})
          gateway.deliver(session, %({"type":"pong"}))
        end

        headers = HTTP::Headers.new
        headers["X-Test"] = "probe"
        client = TestClient.new("ws://127.0.0.1:#{port}/ws?x=1", headers)
        client.send_raw(%({"type":"ping"}))

        sid, path, header, raw = got.receive
        sid.should_not eq("")
        path.should eq("/ws")
        header.should eq("probe")
        raw.should eq(%({"type":"ping"}))

        reply = client.expect { |m| m["type"].as_s == "pong" }
        reply["type"].as_s.should eq("pong")

        gateway.stop
        client.close
      end

      it "on_connect 拒绝接入：会话不注册、不触发 on_disconnect" do
        gateway, stage, port = new_gateway
        gateway.on_connect = ->(session : Session) { session.meta["try"] == "ok" }
        disconnected = Channel(String).new(4)
        gateway.on_disconnect = ->(session : Session) { disconnected.send(session.id) }

        rejected = TestClient.new("ws://127.0.0.1:#{port}/")
        rejected.send_raw(%({"type":"ping"}))
        sleep 250.milliseconds
        gateway.sessions_size.should eq(0)
        try_receive_nb(disconnected, 100.milliseconds).should be_nil
        rejected.close

        gateway.stop
      end

      it "deliver/broadcast 按会话路由；断线触发 on_disconnect 并清理" do
        gateway, stage, port = new_gateway
        disconnects = Channel(String).new(8)
        gateway.on_disconnect = ->(session : Session) { disconnects.send(session.id) }
        ids = {} of String => String
        got = Channel(Tuple(String, String)).new(16) # marker 原文

        gateway.on_message = ->(session : Session, raw : String) do
          ids[raw] = session.id
          got.send({raw, session.id})
          gateway.broadcast(%({"type":"b","to":#{raw.inspect}}))
        end

        a = TestClient.new("ws://127.0.0.1:#{port}/")
        b = TestClient.new("ws://127.0.0.1:#{port}/")
        a.send_raw(%("A"))
        a_id = ids["\"A\""]?
        until a_id
          got.receive
          a_id = ids["\"A\""]?
        end
        b.send_raw(%("B"))
        b_id = ids["\"B\""]?
        until b_id
          got.receive
          b_id = ids["\"B\""]?
        end

        sleep 150.milliseconds
        gateway.sessions_size.should eq(2)

        n = gateway.broadcast(%({"type":"b"}))
        n.should eq(2)
        a.expect { |m| m["type"].as_s == "b" }
        b.expect { |m| m["type"].as_s == "b" }

        payload = JSON.parse(%({"type":"direct"}))
        gateway.deliver_id(a_id.not_nil!, payload).should be_true
        a.expect { |m| m["type"].as_s == "direct" }
        gateway.deliver_id("nonexistent", payload).should be_false

        # 广播排除语义
        n2 = gateway.broadcast(%({"type":"b2"}), except: a_id)
        n2.should eq(1)
        b.expect { |m| m["type"].as_s == "b2" }

        # a 断线：on_disconnect 触发并清理
        a.close
        disconnected_id = disconnects.receive
        disconnected_id.should eq(a_id)
        sleep 150.milliseconds
        gateway.sessions_size.should eq(1)

        gateway.stop
        b.close
      end

      it "溢出策略 Drop：mailbox 满时丢弃消息、会话保留" do
        config = Troupe::Gateway::Config.new(mailbox_cap: 2, push_wait: 50.milliseconds,
          overflow: Troupe::Gateway::Overflow::Drop)
        gateway, stage, port = new_gateway(config)
        session = Session.new("s1", config.mailbox_cap)
        gateway.register(session)

        gateway.deliver(session, %({"n":1})).should be_true
        gateway.deliver(session, %({"n":2})).should be_true
        gateway.deliver(session, %({"n":3})).should be_false
        gateway.sessions_size.should eq(1)

        gateway.stop
      end

      it "溢出策略 Disconnect：mailbox 满时断开会话并触发 on_disconnect" do
        config = Troupe::Gateway::Config.new(mailbox_cap: 2, push_wait: 50.milliseconds,
          overflow: Troupe::Gateway::Overflow::Disconnect)
        gateway, stage, port = new_gateway(config)
        disconnects = Channel(String).new(4)
        gateway.on_disconnect = ->(session : Session) { disconnects.send(session.id) }
        session = Session.new("s2", config.mailbox_cap)
        gateway.register(session)

        gateway.deliver(session, %({"n":1})).should be_true
        gateway.deliver(session, %({"n":2})).should be_true
        gateway.deliver(session, %({"n":3})).should be_false
        disconnects.receive.should eq("s2")
        gateway.sessions_size.should eq(0)
        session.closed_mailbox?.should be_true

        gateway.stop
      end

      it "duplicate? 事件去重；dedup=false 时透传" do
        config = Troupe::Gateway::Config.new(dedup: true)
        gateway, stage, port = new_gateway(config)
        gateway.duplicate?("e1").should be_false
        gateway.duplicate?("e1").should be_true
        gateway.duplicate?("e2").should be_false
        gateway.stop

        config2 = Troupe::Gateway::Config.new(dedup: false)
        gateway2, stage2, port2 = new_gateway(config2)
        gateway2.duplicate?("e1").should be_false
        gateway2.duplicate?("e1").should be_false
        gateway2.stop
      end
    end
  end
end
