package actors

import java.util.concurrent.{ConcurrentLinkedQueue, ArrayBlockingQueue}

/**
 * Configuration for mailbox behavior
 */
case class MailboxConfig(
  capacity: Int = Int.MaxValue,  // Int.MaxValue means unbounded
  deliveryStrategy: DeliveryStrategy = DeliveryStrategy.Unordered
)

/**
 * Message delivery strategies
 */
enum DeliveryStrategy:
  case Unordered
  case Ordered
  case AtLeastOnce
  case AtMostOnce

/**
 * Internal envelope wrapping a message with sender information
 */
case class Envelope(
  message: Message,
  sender: Option[ActorRef],
  retryCount: Int = 0
)

/**
 * Concurrent message queue implementation using Java virtual threads
 *
 * Uses ConcurrentLinkedQueue for unbounded mailboxes (lock-free, high throughput)
 * and ArrayBlockingQueue for bounded mailboxes (backpressure support)
 */
class Mailbox(config: MailboxConfig) {

  private val unboundedQueue: ConcurrentLinkedQueue[Envelope] = new ConcurrentLinkedQueue[Envelope]()
  private val boundedQueue: ArrayBlockingQueue[Envelope] =
    if (config.capacity < Int.MaxValue) new ArrayBlockingQueue[Envelope](config.capacity) else null

  private val isUnbounded: Boolean = config.capacity == Int.MaxValue

  /**
   * Add a message to the mailbox
   */
  def enqueue(message: Message, sender: Option[ActorRef] = None): Boolean = {
    val envelope = Envelope(message, sender)

    if (isUnbounded) {
      unboundedQueue.offer(envelope)
      true
    } else {
      if (boundedQueue.offer(envelope)) {
        true
      } else {
        // Queue is full, handle based on strategy
        config.deliveryStrategy match {
          case DeliveryStrategy.AtMostOnce =>
            false
          case _ =>
            // Block until space available (for bounded queues)
            boundedQueue.put(envelope)
            true
        }
      }
    }
  }

  /**
   * Try to get the next message (non-blocking)
   */
  def dequeue(): Option[Envelope] = {
    if (isUnbounded) {
      Option(unboundedQueue.poll())
    } else {
      Option(boundedQueue.poll())
    }
  }

  /**
   * Block and wait for a message
   */
  def receive(): Envelope = {
    if (isUnbounded) {
      // For unbounded, busy-wait until a message arrives
      var envelope: Envelope = null
      while (envelope == null) {
        Thread.sleep(1)
        envelope = unboundedQueue.poll()
      }
      envelope
    } else {
      boundedQueue.take()
    }
  }

  /**
   * Check if mailbox is empty
   */
  def isEmpty: Boolean = {
    if (isUnbounded) unboundedQueue.isEmpty
    else boundedQueue.isEmpty
  }

  /**
   * Get approximate size (exact for bounded queues, approximate for unbounded)
   */
  def size: Int = {
    if (isUnbounded) unboundedQueue.size()
    else boundedQueue.size()
  }

  /**
   * Clear all messages from the mailbox
   */
  def clear(): Unit = {
    if (isUnbounded) unboundedQueue.clear()
    else boundedQueue.clear()
  }

  /**
   * Drain all messages
   */
  def drain(): List[Envelope] = {
    val drained = scala.collection.mutable.ListBuffer[Envelope]()
    if (isUnbounded) {
      while (!unboundedQueue.isEmpty) {
        val envelope = unboundedQueue.poll()
        if (envelope != null) drained += envelope
      }
    } else {
      while (!boundedQueue.isEmpty) {
        val envelope = boundedQueue.poll()
        if (envelope != null) drained += envelope
      }
    }
    drained.result()
  }
}
