package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SchedulerSpec extends AnyFunSpec with Matchers {

  describe("ActorSystem scheduling") {
    it("should schedule task with MILLISECONDS") {
      val system = new ActorSystem("test-scheduler-ms")
      val executed = scala.collection.mutable.ListBuffer[Unit]()

      system.schedule(100, TimeUnit.MILLISECONDS, () => {
        executed += ()
      })

      Thread.sleep(200)
      executed.size shouldBe 1

      system.shutdown()
    }

    it("should schedule task with SECONDS") {
      val system = new ActorSystem("test-scheduler-seconds")
      val executed = scala.collection.mutable.ListBuffer[Unit]()

      system.schedule(1, TimeUnit.SECONDS, () => {
        executed += ()
      })

      Thread.sleep(1500)
      executed.size shouldBe 1

      system.shutdown()
    }

    it("should schedule task at specific time") {
      val system = new ActorSystem("test-scheduler-at")
      val executed = scala.collection.mutable.ListBuffer[Unit]()

      // scheduleAt uses absolute time, so add delay for scheduling overhead
      val futureTime = System.currentTimeMillis() + 100
      Thread.sleep(10) // Small delay to ensure time is in the future
      system.scheduleAt(futureTime, TimeUnit.MILLISECONDS, () => {
        executed += ()
      })

      // Wait for scheduled execution
      Thread.sleep(200)
      executed.size shouldBe 1

      system.shutdown()
    }

    it("should use different time units correctly") {
      val system = new ActorSystem("test-all-units")
      val results = scala.collection.mutable.ListBuffer[String]()

      system.schedule(50, TimeUnit.MILLISECONDS, () => results += "ms")
      system.schedule(50000, TimeUnit.MICROSECONDS, () => results += "us")

      Thread.sleep(200)
      results.contains("ms") shouldBe true
      results.contains("us") shouldBe true

      system.shutdown()
    }
  }

  describe("Actor with scheduling") {
    it("should use system schedule from within actor") {
      val testSystem = new ActorSystem("test-actor-schedule")
      val scheduledMessage = scala.collection.mutable.ListBuffer[Message]()

      val actor = testSystem.actorOf(Props(() => new Actor {
        override def receive = {
          case "schedule-task" =>
            testSystem.schedule(100, TimeUnit.MILLISECONDS, () => {
              self ! "scheduled-message"
            })
          case msg: String =>
            scheduledMessage += msg
          case _ =>
        }
      }), "scheduler-actor")

      Thread.sleep(100)
      actor ! "schedule-task"
      Thread.sleep(200)

      scheduledMessage.contains("scheduled-message") shouldBe true

      testSystem.shutdown()
    }
  }
}
