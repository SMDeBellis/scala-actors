package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class MailboxAdvancedSpec extends AnyFunSpec with Matchers {

  describe("Bounded Mailbox") {

    it("should limit messages to capacity") {
      val config = MailboxConfig(capacity = 3)
      val mailbox = new Mailbox(config)

      // Fill the mailbox to capacity
      mailbox.enqueue("msg1", None) shouldBe true
      mailbox.enqueue("msg2", None) shouldBe true
      mailbox.enqueue("msg3", None) shouldBe true

      mailbox.size shouldBe 3
    }

    it("should support AtMostOnce delivery strategy") {
      val config = MailboxConfig(
        capacity = 2,
        deliveryStrategy = DeliveryStrategy.AtMostOnce
      )
      val mailbox = new Mailbox(config)

      mailbox.enqueue("msg1", None) shouldBe true
      mailbox.enqueue("msg2", None) shouldBe true

      // Third message should be dropped
      mailbox.enqueue("msg3", None) shouldBe false

      mailbox.size shouldBe 2

      // Verify only first two messages are delivered
      mailbox.dequeue().map(_.message.toString) shouldBe Some("msg1")
      mailbox.dequeue().map(_.message.toString) shouldBe Some("msg2")
      mailbox.dequeue() shouldBe None
    }

    it("should track sender information") {
      val config = MailboxConfig(capacity = 10)
      val mailbox = new Mailbox(config)

      val sender1 = ActorRef(ActorPath("test", "user/sender1"), null, null, None)
      val sender2 = ActorRef(ActorPath("test", "user/sender2"), null, null, None)

      mailbox.enqueue("msg1", Some(sender1))
      mailbox.enqueue("msg2", Some(sender2))
      mailbox.enqueue("msg3", None) // No sender

      val envelope1 = mailbox.dequeue().get
      envelope1.message.toString shouldBe "msg1"
      envelope1.sender shouldBe Some(sender1)

      val envelope2 = mailbox.dequeue().get
      envelope2.message.toString shouldBe "msg2"
      envelope2.sender shouldBe Some(sender2)

      val envelope3 = mailbox.dequeue().get
      envelope3.message.toString shouldBe "msg3"
      envelope3.sender shouldBe None
    }
  }

  describe("Mailbox drain") {

    it("should drain all messages") {
      val config = MailboxConfig(capacity = 10)
      val mailbox = new Mailbox(config)

      // Add several messages
      for (i <- 1 to 5) {
        mailbox.enqueue(s"msg$i", None)
      }

      mailbox.size shouldBe 5

      // Drain all messages
      val drained = mailbox.drain()

      drained.size shouldBe 5
      drained.map(_.message.toString).sorted shouldBe List("msg1", "msg2", "msg3", "msg4", "msg5")

      // Mailbox should be empty after drain
      mailbox.isEmpty shouldBe true
      mailbox.dequeue() shouldBe None
    }

    it("should return empty list for empty mailbox") {
      val mailbox = new Mailbox(MailboxConfig())
      val drained = mailbox.drain()

      drained shouldBe empty
    }

    it("should preserve envelope order during drain") {
      val mailbox = new Mailbox(MailboxConfig(capacity = 10))

      mailbox.enqueue("first", None)
      mailbox.enqueue("second", None)
      mailbox.enqueue("third", None)

      val drained = mailbox.drain()

      drained(0).message.toString shouldBe "first"
      drained(1).message.toString shouldBe "second"
      drained(2).message.toString shouldBe "third"
    }
  }

  describe("Mailbox clear") {

    it("should clear all messages") {
      val mailbox = new Mailbox(MailboxConfig(capacity = 10))

      for (i <- 1 to 10) {
        mailbox.enqueue(s"msg$i", None)
      }

      mailbox.size shouldBe 10

      mailbox.clear()

      mailbox.isEmpty shouldBe true
      mailbox.size shouldBe 0
      mailbox.dequeue() shouldBe None
    }

    it("should be safe to clear empty mailbox") {
      val mailbox = new Mailbox(MailboxConfig())

      // Clearing empty mailbox should not throw
      mailbox.clear()
      mailbox.isEmpty shouldBe true
    }

    it("should allow adding messages after clear") {
      val mailbox = new Mailbox(MailboxConfig(capacity = 10))

      mailbox.enqueue("msg1", None)
      mailbox.clear()
      mailbox.enqueue("msg2", None)

      mailbox.dequeue().map(_.message.toString) shouldBe Some("msg2")
    }
  }

  describe("Mailbox receive (blocking)") {

    it("should block until message arrives") {
      val mailbox = new Mailbox(MailboxConfig())
      var receivedMessage: Option[String] = None

      val thread = new Thread(() => {
        // Block waiting for message
        val envelope = mailbox.receive()
        receivedMessage = Some(envelope.message.toString)
      })
      thread.start()

      Thread.sleep(100) // Ensure thread is blocked
      receivedMessage shouldBe None

      // Send message
      mailbox.enqueue("delayed-message", None)

      thread.join(1000)
      receivedMessage shouldBe Some("delayed-message")
    }

    it("should return immediately if message available") {
      val mailbox = new Mailbox(MailboxConfig(capacity = 10))
      mailbox.enqueue("immediate", None)

      val thread = new Thread(new Runnable {
        def run(): Unit = {
          val envelope = mailbox.receive()
          envelope.message.toString shouldBe "immediate"
        }
      })
      thread.start()
      thread.join(500)

      thread.isAlive shouldBe false // Should complete quickly
    }
  }

  describe("Mailbox with different delivery strategies") {

    it("should handle Unordered strategy (default)") {
      val config = MailboxConfig(
        capacity = 10,
        deliveryStrategy = DeliveryStrategy.Unordered
      )
      val mailbox = new Mailbox(config)

      for (i <- 1 to 5) {
        mailbox.enqueue(s"msg$i", None)
      }

      // Messages can be delivered in any order for unordered
      val messages = scala.collection.mutable.ListBuffer[String]()
      while (!mailbox.isEmpty) {
        messages += mailbox.dequeue().get.message.toString
      }

      messages.size shouldBe 5
    }

    it("should handle Ordered strategy") {
      val config = MailboxConfig(
        capacity = 10,
        deliveryStrategy = DeliveryStrategy.Ordered
      )
      val mailbox = new Mailbox(config)

      for (i <- 1 to 5) {
        mailbox.enqueue(s"msg$i", None)
      }

      // For ordered, messages should be delivered in order
      // (Currently same as unordered in implementation)
      var i = 1
      while (!mailbox.isEmpty) {
        val msg = mailbox.dequeue().get.message.toString
        msg shouldBe s"msg$i"
        i += 1
      }
    }

    it("should handle AtLeastOnce strategy") {
      val config = MailboxConfig(
        capacity = 10,
        deliveryStrategy = DeliveryStrategy.AtLeastOnce
      )
      val mailbox = new Mailbox(config)

      mailbox.enqueue("msg1", None) shouldBe true
      mailbox.enqueue("msg2", None) shouldBe true

      mailbox.size shouldBe 2

      // AtLeastOnce ensures message is delivered at least once
      // (Currently behaves like Ordered in implementation)
    }
  }

  describe("Mailbox isEmpty") {

    it("should return true for new mailbox") {
      val mailbox = new Mailbox(MailboxConfig())
      mailbox.isEmpty shouldBe true
    }

    it("should return false after enqueuing") {
      val mailbox = new Mailbox(MailboxConfig())
      mailbox.isEmpty shouldBe true

      mailbox.enqueue("msg", None)
      mailbox.isEmpty shouldBe false

      mailbox.dequeue()
      mailbox.isEmpty shouldBe true
    }
  }

  describe("Mailbox size") {

    it("should report correct size") {
      val mailbox = new Mailbox(MailboxConfig(capacity = 100))

      mailbox.size shouldBe 0

      for (i <- 1 to 10) {
        mailbox.enqueue(s"msg$i", None)
        mailbox.size shouldBe i
      }

      mailbox.dequeue()
      mailbox.size shouldBe 9
    }
  }
}
