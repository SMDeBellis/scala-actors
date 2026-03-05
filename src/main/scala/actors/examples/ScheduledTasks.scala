package actors.examples

import actors.{Actor, ActorSystem, Message, Props, ActorRef, TimeUnit, Receive}
import scala.concurrent.duration._

/**
 * Demonstrates timer and periodic operations:
 * - One-time scheduled tasks
 * - Periodic/recurring tasks
 * - Scheduled actor messages
 * - Timeout handling
 */
object ScheduledTasks {

  // Messages for scheduling
  case class ScheduledTask(id: Int, description: String) extends AnyRef
  case class Tick(tickNumber: Int) extends AnyRef
  case object Heartbeat extends AnyRef
  case object CleanupExpired extends AnyRef
  case class TaskResult(taskId: Int, result: String) extends AnyRef

  /**
   * Timer actor that schedules one-time tasks
   */
  class TimerActor extends Actor {
    private var pendingTasks = List[ScheduledTask]()

    override def preStart(): Unit = {
      system.log("TimerActor starting")
    }

    override def receive: Receive = {
      case task: ScheduledTask =>
        pendingTasks = task :: pendingTasks
        println(s"[Timer] Scheduled task ${task.id}: ${task.description}")

        // Schedule task execution after a delay
        system.schedule(100, TimeUnit.MILLISECONDS, () => {
          println(s"[Timer] Executing task ${task.id}: ${task.description}")
          // Process task
          pendingTasks = pendingTasks.filter(_ != task)
        })

      case "list" =>
        println(s"[Timer] Pending tasks: ${pendingTasks.map(_.id).mkString(", ")}")

      case "cancel-all" =>
        println(s"[Timer] Cancelled ${pendingTasks.length} tasks")
        pendingTasks = Nil

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Ticker actor that sends periodic messages
   */
  class TickerActor extends Actor {
    private var tickCount = 0
    private var isRunning = false

    override def preStart(): Unit = {
      system.log("TickerActor starting")
    }

    override def receive: Receive = {
      case "start" =>
        if (!isRunning) {
          isRunning = true
          println("[Ticker] Started periodic ticking")
          startTicking()
        }

      case "stop" =>
        isRunning = false
        println("[Ticker] Stopped ticking")

      case Tick(n) =>
        println(s"  [Ticker] Tick #$n received")

      case "status" =>
        println(s"[Ticker] ${if (isRunning) "Running" else "Stopped"}, Total ticks: $tickCount")

      case _ =>
        unhandled(_)
    }

    private def startTicking(): Unit = {
      if (isRunning) {
        tickCount += 1
        println(s"[Ticker] Sending tick #$tickCount")
        self ! Tick(tickCount)
        // Recursively schedule next tick
        system.schedule(500, TimeUnit.MILLISECONDS, () => {
          startTicking()
        })
      }
    }
  }

  /**
   * Watchdog actor that monitors health
   */
  class WatchdogActor extends Actor {
    private var lastHeartbeat = System.currentTimeMillis()
    private val timeoutMs = 2000L

    override def preStart(): Unit = {
      system.log("WatchdogActor starting")
      startMonitoring()
    }

    override def receive: Receive = {
      case Heartbeat =>
        lastHeartbeat = System.currentTimeMillis()
        println(s"[Watchdog] Heartbeat received")

      case "check" =>
        val elapsed = System.currentTimeMillis() - lastHeartbeat
        if (elapsed > timeoutMs) {
          println(s"[Watchdog] WARNING: No heartbeat for ${elapsed}ms (timeout: ${timeoutMs}ms)")
        } else {
          println(s"[Watchdog] OK: Last heartbeat ${elapsed}ms ago")
        }

      case _ =>
        unhandled(_)
    }

    private def startMonitoring(): Unit = {
      val elapsed = System.currentTimeMillis() - lastHeartbeat
      if (elapsed > timeoutMs) {
        println(s"[Watchdog] ALERT: Service appears to be down (${elapsed}ms since last heartbeat)")
      }
      // Recursively schedule next check
      system.schedule(1000, TimeUnit.MILLISECONDS, () => {
        startMonitoring()
      })
    }
  }

  /**
   * Scheduler coordinator
   */
  class Scheduler extends Actor {
    private var scheduledCount = 0

    override def preStart(): Unit = {
      system.log("Scheduler starting")
    }

    private def performPeriodicTask(): Unit = {
      scheduledCount += 1
      if (scheduledCount <= 3) {
        println(s"  [Scheduler] Periodic task #$scheduledCount")
        // Schedule next iteration
        system.schedule(500, TimeUnit.MILLISECONDS, () => {
          performPeriodicTask()
        })
      }
    }

    override def receive: Receive = {
      case "demo-schedule" =>
        println("\n[Scheduler] Scheduling tasks...")

        // One-time task
        system.schedule(500, TimeUnit.MILLISECONDS, () => {
          println("  [Scheduler] One-time task executed!")
        })

        // Delayed message
        system.schedule(1000, TimeUnit.MILLISECONDS, () => {
          println("  [Scheduler] Delayed message sent to self")
          self ! "delayed-response"
        })

        // Periodic task
        performPeriodicTask()

      case "delayed-response" =>

        println("[Scheduler] All tasks scheduled")

      case "delayed-response" =>
        println("  [Scheduler] Received delayed response")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Run scheduled tasks example
   */
  def run(system: ActorSystem): Unit = {
    println("=== Scheduled Tasks Example ===")
    println()

    // One-time scheduling
    println("1. One-time scheduled tasks:")
    val timer = system.actorOf(Props(() => new TimerActor()), "timer")
    timer ! ScheduledTask(1, "Send email notification")
    timer ! ScheduledTask(2, "Process payment")
    timer ! ScheduledTask(3, "Generate report")

    Thread.sleep(300)

    // Ticker/periodic
    println("\n2. Periodic ticker:")
    val ticker = system.actorOf(Props(() => new TickerActor()), "ticker")
    ticker ! "start"

    Thread.sleep(2000)

    ticker ! "stop"
    ticker ! "status"

    Thread.sleep(100)

    // Watchdog/heartbeat
    println("\n3. Watchdog with heartbeat:")
    val watchdog = system.actorOf(Props(() => new WatchdogActor()), "watchdog")

    // Simulate heartbeats
    Thread.sleep(500)
    watchdog ! Heartbeat

    Thread.sleep(500)
    watchdog ! "check"

    Thread.sleep(100)

    // Scheduler demo
    println("\n4. System scheduler:")
    val scheduler = system.actorOf(Props(() => new Scheduler()), "scheduler")
    scheduler ! "demo-schedule"

    Thread.sleep(2500)

    // Cleanup
    println("\n5. Cleanup:")
    timer ! "cancel-all"

    Thread.sleep(100)

    println("\n=== Scheduled Tasks Example Complete ===")
  }
}
