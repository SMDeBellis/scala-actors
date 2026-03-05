package actors

import scala.concurrent.{Future, Promise}
import actors.exceptions.{ActorNotFoundException, DeadLetterException}

/**
 * Immutable reference to an actor
 * Thread-safe and can be passed between actors
 */
class ActorRef private (
  val path: ActorPath,
  private val mailbox: Mailbox,
  private val system: ActorSystem
) extends Equals {

  /**
   * Send a message to this actor (fire-and-forget)
   */
  def ! (message: Message): Unit = tell(message, None)

  /**
   * Send a message with explicit sender
   */
  def tell(message: Message, sender: Option[ActorRef]): Unit = {
    if (mailbox != null) {
      mailbox.enqueue(message, Some(this))
    } else {
      // Actor doesn't exist or is dead
      system.deadLetter(path, message)
    }
  }

  /**
   * Send a message and wait for a response (ask pattern)
   */
  def ? (message: Message, timeout: Long = 5000, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Future[AnyRef] = {
    val promise = Promise[AnyRef]()

    // Create a temporary mailbox for the response
    val responseMailbox = new Mailbox(MailboxConfig())
    val tempPath = ActorPath(system.name, s"temp/${java.util.UUID.randomUUID()}")
    val responseHandlerRef = new ActorRef(tempPath, responseMailbox, system)

    // Start a thread to wait for the response
    new Thread(new java.lang.Runnable {
      override def run(): Unit = {
        // Block waiting for response (with timeout handled by the scheduled task)
        val envelope = responseMailbox.receive()
        envelope.message match {
          case ex: Throwable => promise.tryFailure(ex)
          case response => promise.trySuccess(response)
        }
      }
    }).start()

    // Send the message with the response handler as sender
    tell(message, Some(responseHandlerRef))

    // Schedule timeout
    system.schedule(timeout, timeUnit, () => {
      if (!promise.isCompleted) {
        promise.failure(new scala.concurrent.TimeoutException(s"Timeout waiting for response after ${timeout} ${timeUnit}"))
      }
    })

    promise.future
  }

  /**
   * Forward a message to another actor (preserves original sender)
   */
  def forward(message: Message, target: ActorRef): Unit = {
    target.tell(message, Some(this))
  }

  /**
   * Check if this ref equals another
   */
  override def canEqual(that: Any): Boolean = that.isInstanceOf[ActorRef]

  override def hashCode(): Int = path.hashCode()

  override def equals(that: Any): Boolean = {
    that match {
      case other: ActorRef => canEqual(that) && path == other.path
      case _ => false
    }
  }

  override def toString: String = path.toString
}

object ActorRef {
  def apply(path: ActorPath, mailbox: Mailbox, system: ActorSystem): ActorRef = {
    new ActorRef(path, mailbox, system)
  }

  /**
   * Create a temporary actor ref (used for ask pattern)
   */
  def temporary(system: ActorSystem): ActorRef = {
    val path = ActorPath(system.name, s"temp/${java.util.UUID.randomUUID()}")
    new ActorRef(path, null, system)
  }
}
