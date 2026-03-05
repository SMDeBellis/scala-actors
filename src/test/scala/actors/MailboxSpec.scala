package actors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import scala.concurrent.duration._

/**
 * Tests for the Mailbox concurrent message queue
 */
class MailboxSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var mailbox: Mailbox = _

  override def beforeEach(): Unit = {
    mailbox = new Mailbox(MailboxConfig())
  }

  "A Mailbox" should "be empty when created" in {
    mailbox.isEmpty shouldBe true
    mailbox.size shouldBe 0
  }

  it should "enqueue and dequeue messages in order" in {
    mailbox.enqueue("message1")
    mailbox.enqueue("message2")
    mailbox.enqueue("message3")

    mailbox.size shouldBe 3
    mailbox.isEmpty shouldBe false

    val msg1 = mailbox.dequeue().get.message
    val msg2 = mailbox.dequeue().get.message
    val msg3 = mailbox.dequeue().get.message

    msg1 shouldBe "message1"
    msg2 shouldBe "message2"
    msg3 shouldBe "message3"

    mailbox.isEmpty shouldBe true
  }

  it should "preserve sender information" in {
    val sender = ActorRef.temporary(ActorSystem("test"))
    mailbox.enqueue("message", Some(sender))

    val envelope = mailbox.dequeue().get
    envelope.message shouldBe "message"
    envelope.sender shouldBe defined
    envelope.sender.get should equal(sender)
  }

  it should "support concurrent enqueue from multiple threads" in {
    import java.util.concurrent.Executors
    val executor = Executors.newFixedThreadPool(10)
    val numMessages = 1000

    // Enqueue messages from multiple threads
    val futures = (0 until numMessages).map { i =>
      executor.submit(() => {
        mailbox.enqueue(s"message-$i")
      })
    }

    import java.util.concurrent.TimeUnit as JavaTimeUnit
    // Wait for all enqueues
    futures.foreach(_.get(10, JavaTimeUnit.SECONDS))

    // All messages should be in the mailbox
    mailbox.size shouldBe numMessages
    mailbox.isEmpty shouldBe false

    executor.shutdown()
  }

  it should "drain all messages" in {
    mailbox.enqueue("msg1")
    mailbox.enqueue("msg2")
    mailbox.enqueue("msg3")

    val drained = mailbox.drain()

    drained.length shouldBe 3
    mailbox.isEmpty shouldBe true
  }

  it should "clear all messages" in {
    mailbox.enqueue("msg1")
    mailbox.enqueue("msg2")
    mailbox.clear()

    mailbox.isEmpty shouldBe true
    mailbox.dequeue() shouldBe None
  }

  it should "support bounded mailbox" in {
    val boundedMailbox = new Mailbox(MailboxConfig(capacity = 3))

    boundedMailbox.enqueue("msg1") shouldBe true
    boundedMailbox.enqueue("msg2") shouldBe true
    boundedMailbox.enqueue("msg3") shouldBe true
    boundedMailbox.size shouldBe 3

    // Next enqueue should block or fail depending on strategy
    boundedMailbox.dequeue() shouldBe defined
  }
}
