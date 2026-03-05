package actors

import java.util.concurrent.{Executors, ScheduledExecutorService, ConcurrentHashMap, ExecutorService}
import scala.concurrent.duration.Duration
import actors.exceptions.{ActorNotFoundException, ActorStartException}

import java.util.concurrent.TimeUnit as JavaTimeUnit

/**
 * TimeUnit for scheduling
 */
enum TimeUnit {
  case NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS

  def toJava: JavaTimeUnit = this match {
    case NANOSECONDS => JavaTimeUnit.NANOSECONDS
    case MICROSECONDS => JavaTimeUnit.MICROSECONDS
    case MILLISECONDS => JavaTimeUnit.MILLISECONDS
    case SECONDS => JavaTimeUnit.SECONDS
    case MINUTES => JavaTimeUnit.MINUTES
    case HOURS => JavaTimeUnit.HOURS
    case DAYS => JavaTimeUnit.DAYS
  }
}

/**
 * Supervisor strategy for handling actor failures
 */
enum SupervisorStrategy {
  case Restart
  case Stop
  case Resume
  case Escalate
}

case class ActorSettings(
  supervisorStrategy: SupervisorStrategy = SupervisorStrategy.Restart,
  restartMaxRetries: Int = 3,
  restartBackoff: Long = 1000,
  restartBackoffTimeUnit: TimeUnit = TimeUnit.MILLISECONDS
)

/**
 * ActorSystem - the root of the actor hierarchy
 *
 * Manages actor lifecycle, creates virtual threads for message processing,
 * and provides the bootstrap mechanism for actor creation.
 */
class ActorSystem(val name: String) extends AutoCloseable {

  private val actors = new ConcurrentHashMap[ActorPath, ActorRef]()
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(4)
  private var isShutdown = false

  /**
   * Create an actor with the given props at the root level
   */
  def actorOf(props: Props, name: String): ActorRef = {
    val path = ActorPath(name, name)
    createActor(props, path)
  }

  /**
   * Create an actor at a specific path
   */
  private def createActor(props: Props, path: ActorPath): ActorRef = {
    if (isShutdown) {
      throw new IllegalStateException(s"ActorSystem '$name' is shut down")
    }

    // Check if actor already exists
    val existing = actors.get(path)
    if (existing != null) return existing

    try {
      // Create actor instance
      val actor = props.actorFactory()

      // Create mailbox
      val mailbox = new Mailbox(props.mailboxConfig)

      // Create actor ref
      val actorRef = ActorRef(path, mailbox, this)

      // Initialize actor
      actor.self = actorRef
      actor.mailbox = mailbox
      actor.system = this

      // Register actor
      actors.put(path, actorRef)

      // Start actor in a Java virtual thread (Java 21+)
      Thread.startVirtualThread(new Runnable {
        override def run(): Unit = {
          Thread.currentThread().setName(s"${name}-${path.name}")
          actor.start()
        }
      })

      actorRef

    } catch {
      case ex: Exception =>
        actors.remove(path)
        throw new ActorStartException(
          s"Failed to start actor at path: ${path}", ex)
    }
  }

  /**
   * Stop an actor gracefully
   */
  def stop(ref: ActorRef): Unit = {
    // Send shutdown message
    ref ! Shutdown

    // Wait for actor to stop (simplified - could use future/promise)
    Thread.sleep(100)

    // Cleanup
    actors.remove(ref.path)
  }

  /**
   * Lookup an actor by path string
   */
  def selector(path: String): Option[ActorRef] = {
    val actorPath = ActorPath(name, path)
    Option(actors.get(actorPath))
  }

  /**
   * Schedule a task for delayed execution
   */
  def schedule(delay: Long, unit: TimeUnit, task: () => Unit): Unit = {
    scheduler.schedule(new Runnable {
      def run(): Unit = task()
    }, delay, unit.toJava)
  }

  /**
   * Schedule a task for execution at a specific time
   */
  def scheduleAt(time: Long, unit: TimeUnit, task: () => Unit): Unit = {
    scheduler.schedule(new Runnable {
      def run(): Unit = task()
    }, time, unit.toJava)
  }

  /**
   * Handle dead letters
   */
  private[actors] def deadLetter(path: ActorPath, message: Message): Unit = {
    log(s"DEAD LETTER to ${path}: ${message.getClass.getSimpleName}")
  }

  /**
   * Logging helper
   */
  def log(message: String): Unit = {
    println(s"[$name] $message")
  }

  /**
   * Shutdown the actor system
   */
  def shutdown(): Unit = {
    if (isShutdown) return
    isShutdown = true

    // Stop all actors
    actors.values().forEach((ref: ActorRef) => {
      try {
        ref ! Shutdown
      } catch {
        case _: Exception =>
      }
    })

    // Wait for actors to finish
    Thread.sleep(500)

    // Shutdown scheduler
    scheduler.shutdown()
    scheduler.awaitTermination(10, JavaTimeUnit.SECONDS)

    // Clear actors map
    actors.clear()
  }

  override def close(): Unit = shutdown()
}

object ActorSystem {

  /**
   * Create a new ActorSystem with the given name
   */
  def apply(name: String): ActorSystem = new ActorSystem(name)

  /**
   * Get the current actor system (must be called from within an actor context)
   */
  def current: ActorSystem = {
    // This would need thread-local storage or similar mechanism
    // For now, throw an exception
    throw new IllegalStateException(
      "No current ActorSystem available. Pass it explicitly or call from actor context.")
  }
}
