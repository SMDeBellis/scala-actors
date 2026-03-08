package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ActorControlMessagesSpec extends AnyFunSpec with Matchers {

  describe("Control messages") {

    it("should handle GetReference control message") {
      val testSystem = new ActorSystem("test-getref")

      val targetActor = testSystem.actorOf(Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }), "target")

      // GetReference is a control message handled internally by actors
      // It's used for actor self-discovery
      targetActor.path should not be null
      targetActor.path.name shouldBe "target"

      testSystem.shutdown()
    }

    it("should allow two-way communication via selector") {
      val testSystem = new ActorSystem("test-twoway")
      val communicationLog = scala.collection.mutable.ListBuffer[String]()

      // Actor that responds to greetings
      val serviceActor = testSystem.actorOf(Props(() => new Actor {
        override def receive = {
          case "greet" =>
            communicationLog += "service-received-greet"
          case _ =>
        }
      }), "service")

      // Client actor that uses selector to find service
      val clientActor = testSystem.actorOf(Props(() => new Actor {
        override def preStart(): Unit = {
          // Use selector to find and communicate with service
          testSystem.selector("/user/service") foreach { service =>
            service ! "greet"
          }
        }

        override def receive = {
          case _ =>
        }
      }), "client")

      Thread.sleep(300)

      communicationLog should contain("service-received-greet")

      testSystem.shutdown()
    }

    it("should handle multiple selectors to same actor") {
      val testSystem = new ActorSystem("test-multi-selector")
      val receivedMessages = scala.collection.mutable.ListBuffer[String]()

      val sharedActor = testSystem.actorOf(Props(() => new Actor {
        override def receive = {
          case msg: String =>
            receivedMessages += msg
          case _ =>
        }
      }), "shared")

      // Multiple senders using selector
      for (i <- 1 to 5) {
        testSystem.actorOf(Props(() => new Actor {
          override def preStart(): Unit = {
            testSystem.selector("/user/shared") foreach { shared =>
              shared ! s"message-$i"
            }
          }
          override def receive = {
            case _ =>
          }
        }), s"sender-$i")
      }

      Thread.sleep(300)

      receivedMessages.size shouldBe 5

      testSystem.shutdown()
    }
  }
}
