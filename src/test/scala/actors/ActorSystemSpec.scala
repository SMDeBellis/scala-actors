package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ActorSystemSpec extends AnyFunSpec with Matchers {

  describe("ActorSystem companion") {

    it("should create ActorSystem via apply") {
      val system = ActorSystem("test-apply")

      system.name shouldBe "test-apply"
      system should not be null

      system.shutdown()
    }

    it("should create systems with unique names") {
      val system1 = ActorSystem("system-one")
      val system2 = ActorSystem("system-two")

      system1.name shouldBe "system-one"
      system2.name shouldBe "system-two"
      system1 should not be system2

      system1.shutdown()
      system2.shutdown()
    }

    it("should create ActorSystem and immediately create actors") {
      val system = ActorSystem("test-immediate")

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }), "actor")

      actor should not be null

      system.shutdown()
    }
  }

  describe("ActorSystem lifecycle") {

    it("should log messages") {
      val system = new ActorSystem("test-log")

      // Log method should not throw
      noException should be thrownBy {
        system.log("test message")
      }

      system.shutdown()
    }

    it("should handle dead letters") {
      val system = new ActorSystem("test-deadletter")

      // Create a reference to a non-existent actor path
      val nonExistentPath = ActorPath("test-deadletter", "user/nonexistent")

      // DeadLetter method should not throw
      noException should be thrownBy {
        system.deadLetter(nonExistentPath, "message")
      }

      system.shutdown()
    }
  }

  describe("ActorSystem actor creation") {

    it("should throw when creating actor on shutdown system") {
      val system = new ActorSystem("test-shutdown-create")
      system.shutdown()

      val exception = the[IllegalStateException] thrownBy system.actorOf(Props(() => new Actor {
        override def receive = { case _ => }
      }), "actor")
      exception.getMessage should include("shut down")
    }
  }
}
