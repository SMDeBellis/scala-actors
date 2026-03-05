package actors.examples

import actors.{Actor, ActorSystem, Message, Props, ActorRef, MailboxConfig, Receive}

/**
 * Demonstrates backpressure handling:
 * - Bounded mailboxes
 * - Flow control
 * - Graceful degradation
 * - Overflow handling
 */
object BackpressureDemo {

  // Messages for backpressure demo
  case class WorkUnit(id: Int, payload: String) extends AnyRef
  case object BackPressured
  case object FlowResumed
  case class ProcessingRate(rate: Int)

  /**
   * Producer that generates work units
   */
  class Producer(target: ActorRef) extends Actor {
    private var producedCount = 0
    private var blockedCount = 0
    private val workUnitsPerBatch = 10

    override def preStart(): Unit = {
      system.log("Producer starting")
    }

    override def receive: Receive = {
      case "produce" =>
        println(s"[Producer] Producing batch of $workUnitsPerBatch work units...")
        (1 to workUnitsPerBatch).foreach { i =>
          producedCount += 1
          val workId = producedCount
          try {
            target ! WorkUnit(workId, s"payload-$workId")
            println(s"  - Sent WorkUnit($workId)")
          } catch {
            case _: Exception =>
              blockedCount += 1
              println(s"  - Blocked! WorkUnit($workId) (blocked count: $blockedCount)")
          }
        }
        println(s"[Producer] Batch complete (produced: $producedCount, blocked: $blockedCount)")

      case "status" =>
        println(s"[Producer] Produced: $producedCount, Blocked: $blockedCount")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Consumer with bounded mailbox
   */
  class Consumer extends Actor {
    private var processedCount = 0
    private var droppedCount = 0

    override def preStart(): Unit = {
      system.log("Consumer starting with bounded mailbox")
    }

    override def receive: Receive = {
      case WorkUnit(id, payload) =>
        processedCount += 1
        println(s"[Consumer] Processing WorkUnit $id: $payload (total: $processedCount)")

        // Simulate processing time
        Thread.sleep(50)

      case "status" =>
        println(s"[Consumer] Processed: $processedCount, Dropped: $droppedCount")

      case "clear" =>
        processedCount = 0
        droppedCount = 0
        println("[Consumer] Counters cleared")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Buffering actor that handles overflow
   */
  class BufferingActor extends Actor {
    private var buffer = List[WorkUnit]()
    private val maxBufferSize = 5
    private var droppedCount = 0

    override def preStart(): Unit = {
      system.log(s"BufferingActor starting (max buffer: $maxBufferSize)")
    }

    override def receive: Receive = {
      case WorkUnit(id, payload) =>
        if (buffer.size < maxBufferSize) {
          buffer = WorkUnit(id, payload) :: buffer
          println(s"[Buffer] Stored WorkUnit $id (buffer size: ${buffer.size})")
        } else {
          droppedCount += 1
          println(s"[Buffer] DROPPED WorkUnit $id (buffer full, dropped: $droppedCount)")
        }

      case "flush" =>
        println(s"[Buffer] Flushing ${buffer.size} buffered units...")
        buffer.reverse.foreach { unit =>
          println(s"  - Flushing WorkUnit ${unit.id}")
        }
        buffer = Nil

      case "buffer-status" =>
        println(s"[Buffer] Current size: ${buffer.size}/${maxBufferSize}")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Rate-limited processor
   */
  class RateLimiter(target: ActorRef) extends Actor {
    private var messagesHeld = 0
    private val maxRate = 3  // Messages per interval
    private val intervalMs = 500L

    override def preStart(): Unit = {
      system.log("RateLimiter starting")
    }

    override def receive: Receive = {
      case WorkUnit(id, payload) =>
        messagesHeld += 1
        println(s"[RateLimiter] Received WorkUnit $id (held: $messagesHeld)")

        if (messagesHeld <= maxRate) {
          target ! WorkUnit(id, payload)
          messagesHeld = 0
        }

      case "release" =>
        println(s"[RateLimiter] Releasing held messages ($messagesHeld)")
        messagesHeld = 0

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Run backpressure demo
   */
  def run(system: ActorSystem): Unit = {
    println("=== Backpressure Demo ===")
    println()

    // Demo 1: Bounded mailbox
    println("1. Bounded mailbox (capacity: 3):")
    val boundedConsumer = system.actorOf(
      Props(() => new Consumer(), MailboxConfig(capacity = 3)),
      "bounded-consumer"
    )

    Thread.sleep(50)

    // Demo 2: Producer-consumer with backpressure
    println("\n2. Producer-consumer flow:")
    val consumer = system.actorOf(Props(() => new Consumer()), "consumer")
    val producer = system.actorOf(Props(() => new Producer(consumer)), "producer")

    Thread.sleep(50)

    producer ! "produce"

    Thread.sleep(800)

    // Demo 3: Buffering
    println("\n3. Buffering with overflow:")
    val buffer = system.actorOf(Props(() => new BufferingActor()), "buffer")

    (1 to 7).foreach { i =>
      buffer ! WorkUnit(i, s"buffered-$i")
    }

    Thread.sleep(100)

    buffer ! "buffer-status"
    buffer ! "flush"

    Thread.sleep(100)

    // Demo 4: Rate limiting
    println("\n4. Rate limiting:")
    val rateLimiter = system.actorOf(Props(() => new RateLimiter(consumer)), "rate-limiter")

    Thread.sleep(50)

    (1 to 5).foreach { i =>
      rateLimiter ! WorkUnit(i, s"rate-limited-$i")
    }

    Thread.sleep(600)

    // Statistics
    println("\n5. Final statistics:")
    producer ! "status"
    consumer ! "status"

    Thread.sleep(100)

    println("\n=== Backpressure Demo Complete ===")
  }
}
