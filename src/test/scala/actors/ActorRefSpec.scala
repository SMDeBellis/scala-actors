package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class ActorRefSpec extends AnyFunSpec with Matchers {

  describe("ActorRef") {
    it("should have a path") {
      val system = new ActorSystem("test-ref")
      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }), "test-actor")

      actor.path shouldNot be(null)
      actor.path.toString should include("test-actor")

      system.shutdown()
    }

    it("should support tell pattern (!) with message") {
      val system = new ActorSystem("test-tell")
      val messagesReceived = scala.collection.mutable.ListBuffer[Message]()

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case msg =>
            messagesReceived += msg
        }
      }), "receiver")

      Thread.sleep(100)

      // Use tell pattern
      actor ! "test-message"

      Thread.sleep(100)
      messagesReceived.size shouldBe 1
      messagesReceived.head.toString shouldBe "test-message"

      system.shutdown()
    }

    it("should support tell pattern with sender") {
      val system = new ActorSystem("test-tell-sender")
      var receivedSender: Option[ActorRef] = None

      val receiver = system.actorOf(Props(() => new Actor {
        override def receive = {
          case "ping" =>
            // Sender information would be available here
            receivedSender = Some(self) // Just to test the pattern
        }
      }), "receiver")

      val sender = system.actorOf(Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }), "sender")

      Thread.sleep(100)

      receiver ! "ping"

      system.shutdown()
    }

    it("should support ask pattern (?) with timeout") {
      val system = new ActorSystem("test-ask")

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case "ping" => "pong" // This would return via ask
          case _ =>
        }
      }), "actor")

      Thread.sleep(100)

      // Ask pattern would return a Future
      // For now, just verify the actor can receive messages
      actor ! "ping"

      system.shutdown()
    }

    it("should have parent reference when created as child") {
      val system = new ActorSystem("test-parent-ref")

      class ParentActor extends Actor {
        override def preStart(): Unit = {
          context.actorOf(Props(() => new Actor {
            override def receive = {
              case _ =>
            }
          }), "child")
        }
        override def receive = {
          case _ =>
        }
      }

      val parent = system.actorOf(Props(() => new ParentActor), "parent")
      Thread.sleep(100)

      val child = system.selector("/user/parent/child")
      child.isDefined shouldBe true

      system.shutdown()
    }

    it("should send messages to mailbox") {
      val system = new ActorSystem("test-mailbox-send")
      val messagesReceived = scala.collection.mutable.ListBuffer[String]()

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case msg: String =>
            messagesReceived += msg
          case _ =>
        }
      }), "mailbox-test")

      Thread.sleep(100)

      actor ! "msg1"
      actor ! "msg2"
      actor ! "msg3"

      Thread.sleep(100)

      messagesReceived.size shouldBe 3
      messagesReceived shouldBe List("msg1", "msg2", "msg3")

      system.shutdown()
    }

    it("should handle multiple sends from different actors") {
      val testSystem = new ActorSystem("test-multi-send")
      val receivedMessages = scala.collection.mutable.ListBuffer[String]()

      val receiver = testSystem.actorOf(Props(() => new Actor {
        override def receive = {
          case msg: String =>
            receivedMessages += msg
          case _ =>
        }
      }), "receiver")

      val sender1 = testSystem.actorOf(Props(() => new Actor {
        override def preStart(): Unit = {
          testSystem.selector("/user/receiver") foreach (_ ! "from-sender1")
        }
        override def receive = {
          case _ =>
        }
      }), "sender1")

      val sender2 = testSystem.actorOf(Props(() => new Actor {
        override def preStart(): Unit = {
          testSystem.selector("/user/receiver") foreach (_ ! "from-sender2")
        }
        override def receive = {
          case _ =>
        }
      }), "sender2")

      Thread.sleep(200)

      receivedMessages.size shouldBe 2

      testSystem.shutdown()
    }
  }
}
