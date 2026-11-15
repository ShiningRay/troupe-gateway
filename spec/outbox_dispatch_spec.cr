require "./spec_helper"

# Outbox 派发循环：track 的 actor 事件 → on_outbox（经 event_id 去重）。
class EmitterProps
  include JSON::Serializable
  property n : Int32 = 0

  def initialize
  end
end

class EmitterActor < Troupe::Actor(EmitterProps)
  troupe_manifest "emitter", schema_version: 1 do
    turn emit, {} of String => String, returns: Int32
  end

  def emit : Int32
    props.n += 1
    emit_event("ping", JSON.parse(%({"n":#{props.n}})))
    save
    props.n
  end
end

module Troupe
  module Gateway
    describe "Outbox 派发" do
      it "track 的 actor 事件经派发循环送达 on_outbox" do
        stage = Troupe::Stage.new
        gateway = WebSocket.new(stage.store, Config.new(dispatch_interval: 25.milliseconds))
        records = Channel(OutboxRecord).new(16)
        gateway.on_outbox = ->(record : OutboxRecord) { records.send(record) }

        stage.register(EmitterActor.troupe_manifest)
        port = gateway.listen("127.0.0.1", 0)
        id = ActorId.new("gw", "emitter", "1")
        gateway.track(id)
        gateway.tracked_size.should eq(1)

        agent = EmitterActor::Agent.new(stage)
        agent.emit(id).should eq(1)

        deadline = Time.instant + 3.seconds
        record = nil
        while record.nil? && Time.instant < deadline
          select
          when r = records.receive
            record = r
          when timeout(deadline - Time.instant)
            break
          end
        end

        record.should_not be_nil
        record.not_nil!.topic.should eq("ping")
        record.not_nil!.payload["n"].as_i.should eq(1)

        gateway.untrack(id)
        gateway.tracked_size.should eq(0)
        gateway.stop
      end
    end
  end
end
