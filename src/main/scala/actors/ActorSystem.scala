package actors

import java.util.concurrent.{Executors, ScheduledExecutorService, ConcurrentHashMap, ExecutorService}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
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
  private val children = new ConcurrentHashMap[ActorPath, java.util.List[ActorRef]]()
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(4)
  private var isShutdown = false

  // Create /user guardian - root for all user-level actors
  private val userPath = ActorPath(name, "user")
  private val userGuardian = ActorRef(userPath, null, this, None)
  actors.put(userPath, userGuardian)

  /**
   * Create an actor with the given props at the root level (child of /user)
   */
  def actorOf(props: Props, name: String): ActorRef = {
    // Actors created at root level become children of /user
    val path = ActorPath(this.name, s"user/$name")
    createActor(props, path)
  }

  /**
   * Create a child actor under a parent path
   */
  def actorOf(parentPath: ActorPath, props: Props, name: String): ActorRef = {
    val childPath = parentPath.child(name)
    createActor(props, childPath)
  }

  /**
   * Create a child actor under a parent ActorRef
   */
  def actorOf(parentRef: ActorRef, props: Props, name: String): ActorRef = {
    actorOf(parentRef.path, props, name)
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

      // Get parent reference
      val parentRef = path.parent.flatMap(p => Option(actors.get(p)))

      // Create actor ref WITH parent reference
      val actorRef = ActorRef(path, mailbox, this, parentRef)

      // Initialize actor
      actor.self = actorRef
      actor.mailbox = mailbox
      actor.system = this

      // Register actor
      actors.put(path, actorRef)

      // Register with parent
      path.parent.foreach(parentPath => addChild(parentPath, actorRef))

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

  // Get all children of an actor, empty list if none
  private def getChildren(path: ActorPath): List[ActorRef] = {
    Option(children.get(path)).map(_.asScala.toList).getOrElse(Nil)
  }

  // Add child to parent's children list
  private def addChild(parentPath: ActorPath, childRef: ActorRef): Unit = {
    val parentChildren = Option(children.get(parentPath))
      .getOrElse {
        val list = new java.util.ArrayList[ActorRef]()
        children.put(parentPath, list)
        list
      }
    parentChildren.add(childRef)
  }

  // Remove child from parent's children list
  private def removeChild(parentPath: ActorPath, childRef: ActorRef): Unit = {
    Option(children.get(parentPath)).foreach(_.remove(childRef))
  }

  /**
   * Stop an actor gracefully (cascades to children first)
   */
  def stop(ref: ActorRef): Unit = {
    // Recursively stop all children first
    val childRefs = getChildren(ref.path)
    childRefs.foreach { child =>
      stop(child)
    }

    // Then stop this actor
    ref ! Shutdown

    // Wait for actor to stop (simplified - could use future/promise)
    Thread.sleep(100)

    // Remove from parent's children list
    ref.parent.foreach { p =>
      removeChild(p.path, ref)
    }

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

  // Get children of an actor (public API)
  def children(ref: ActorRef): List[ActorRef] = getChildren(ref.path)

  // Check if actor has children
  def hasChildren(path: ActorPath): Boolean = {
    Option(children.get(path)).map(!_.isEmpty).getOrElse(false)
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

    // Get root-level actors (children of /user)
    val rootActors = getChildren(userPath)

    // Stop root actors (will cascade to all children)
    rootActors.foreach { ref =>
      try { stop(ref) } catch { case _: Exception => }
    }

    // Wait for actors to finish
    Thread.sleep(500)

    // Shutdown scheduler
    scheduler.shutdown()
    scheduler.awaitTermination(10, JavaTimeUnit.SECONDS)

    // Clear actors map and children map
    actors.clear()
    children.clear()
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
