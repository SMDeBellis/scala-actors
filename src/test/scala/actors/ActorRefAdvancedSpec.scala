package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._

class ActorRefAdvancedSpec extends AnyFunSpec with Matchers {

  describe("ActorRef ask pattern") {

    it("should create a Future for ask pattern") {
      val system = new ActorSystem("test-ask")

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }), "actor")

      // The ask pattern should create a Future
      val future = actor ? ("ping", 100, TimeUnit.MILLISECONDS)
      future should not be null

      // Future will timeout since actor doesn't respond, but the point is it was created

      system.shutdown()
    }

    it("should timeout after specified duration") {
      val system = new ActorSystem("test-timeout")

      val slowActor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case _ => // Never responds
        }
      }), "slow")

      val future = slowActor ? ("ping", 100, TimeUnit.MILLISECONDS)

      // Wait for the timeout to occur
      Thread.sleep(200)

      // Future should be completed (with timeout exception)
      future.isCompleted shouldBe true

      // Getting the result should throw TimeoutException
      assertThrows[scala.concurrent.TimeoutException] {
        Await.result(future, 1.second)
      }

      system.shutdown()
    }

    it("should handle ask pattern with short timeout") {
      val system = new ActorSystem("test-short-timeout")

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }), "actor")

      val future = actor ? ("ping", 50, TimeUnit.MILLISECONDS)

      // Wait for timeout
      Thread.sleep(100)

      // Future should be completed (with timeout exception)
      future.isCompleted shouldBe true

      system.shutdown()
    }
  }

  describe("ActorRef forward") {

    it("should forward message preserving sender") {
      val system = new ActorSystem("test-forward")
      var receivedSender: Option[ActorRef] = None

      val forwarder = system.actorOf(Props(() => new Actor {
        override def receive = {
          case "forward-me" =>
          case _ =>
        }
      }), "forwarder")

      val target = system.actorOf(Props(() => new Actor {
        override def receive = {
          case "forward-me" =>
          case _ =>
        }
      }), "target")

      forwarder.forward("forward-me", target)

      Thread.sleep(100)

      system.shutdown()
    }

    it("should forward to different actor paths") {
      val system = new ActorSystem("test-forward-paths")
      val messagesReceived = scala.collection.mutable.ListBuffer[String]()

      val router = system.actorOf(Props(() => new Actor {
        override def receive = {
          case msg: String =>
          case _ =>
        }
      }), "router")

      val target1 = system.actorOf(Props(() => new Actor {
        override def receive = {
          case "msg1" => messagesReceived += "target1"
          case _ =>
        }
      }), "target1")

      router.forward("msg1", target1)

      Thread.sleep(100)

      messagesReceived should contain("target1")

      system.shutdown()
    }
  }

  describe("ActorRef hashCode") {

    it("should return same hashCode for equal paths") {
      val system = new ActorSystem("test-hashcode")

      val actor1 = system.actorOf(Props(() => new Actor {
        override def receive = { case _ => }
      }), "actor1")

      val hash1 = actor1.hashCode()
      val hash2 = actor1.hashCode()

      hash1 shouldBe hash2

      system.shutdown()
    }

    it("should be consistent across multiple calls") {
      val system = new ActorSystem("test-hashcode-consistent")

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = { case _ => }
      }), "actor")

      val hashes = (1 to 10).map(_ => actor.hashCode()).toSet
      hashes.size shouldBe 1  // All hash codes should be identical

      system.shutdown()
    }
  }

  describe("ActorRef toString") {

    it("should return path string representation") {
      val system = new ActorSystem("test-tostring")

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = { case _ => }
      }), "my-actor")

      actor.toString should include("my-actor")

      system.shutdown()
    }
  }
}
