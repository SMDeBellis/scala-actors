package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PropsSpec extends AnyFunSpec with Matchers {

  describe("Props factory") {

    it("should create Props from actor factory function") {
      val props = Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      })

      props.actorFactory should not be null
      val actor = props.actorFactory()
      actor shouldBe an[Actor]
    }

    it("should create Props from Actor class") {
      class TestActor extends Actor {
        override def receive = {
          case _ =>
        }
      }

      val props = Props(classOf[TestActor])
      props.actorFactory should not be null

      val actor = props.actorFactory()
      actor shouldBe a[TestActor]
    }

    it("should create Props from PartialFunction") {
      val behavior: PartialFunction[Message, Unit] = {
        case "ping" => println("pong")
        case _ => println("unknown")
      }

      val props = Props(behavior)
      props.actorFactory should not be null

      val actor = props.actorFactory()
      actor shouldBe an[FunctionActor]
    }

    it("should create Props with custom MailboxConfig") {
      val customConfig = MailboxConfig(capacity = 100)
      val props = Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }, customConfig)

      props.mailboxConfig.capacity shouldBe 100
    }

    it("should create Props with default MailboxConfig") {
      val props = Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      })

      props.mailboxConfig.capacity shouldBe Int.MaxValue
      props.mailboxConfig.deliveryStrategy shouldBe DeliveryStrategy.Unordered
    }

    it("should support Props immutability") {
      val props = Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      })

      // Case class is immutable
      val factory = props.actorFactory
      val config = props.mailboxConfig

      factory should not be null
      config should not be null
    }
  }

  describe("FunctionActor") {

    it("should handle messages via PartialFunction") {
      val behavior: PartialFunction[Message, Unit] = {
        case "test" => // handler
      }

      val props = Props(behavior)
      val actor = props.actorFactory()

      actor.receive.isDefinedAt("test") shouldBe true
      actor.receive.isDefinedAt("unknown") shouldBe false
    }
  }
}
