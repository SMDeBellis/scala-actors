package actors

import scala.util.{Try, Success, Failure}

/**
 * Type alias for message handler
 */
type Receive = PartialFunction[Message, Unit]

/**
 * Context provided to actors for system interaction
 */
class ActorContext(
  val system: ActorSystem,
  val self: ActorRef
) {

  /**
   * Get or create a child actor
   */
  def actorOf(props: Props, name: String): ActorRef = {
    // Create child at proper hierarchical path using parent ref
    system.actorOf(self, props, name)
  }

  /**
   * Get all child actors
   */
  def children(): List[ActorRef] = {
    system.children(self)
  }

  /**
   * Stop an actor
   */
  def stop(ref: ActorRef): Unit = {
    system.stop(ref)
  }

  /**
   * Lookup an actor by path
   */
  def actorSelection(path: String): Option[ActorRef] = {
    system.selector(path)
  }
}

/**
 * Base Actor class
 *
 * Each actor runs its message processing loop in a Java virtual thread,
 * ensuring isolation and high concurrency.
 */
abstract class Actor {

  /**
   * The actor's own reference
   */
  var self: ActorRef = _

  /**
   * The actor's mailbox
   */
  var mailbox: Mailbox = _

  /**
   * The actor system
   */
  var system: ActorSystem = _

  /**
   * Context for this actor
   */
  lazy val context: ActorContext = new ActorContext(system, self)

  /**
   * Message handler - must be implemented by subclasses
   */
  def receive: Receive

  /**
   * Called when actor starts
   */
  def preStart(): Unit = {}

  /**
   * Called when actor stops
   */
  def postStop(): Unit = {}

  /**
   * Called when actor receives an unhandled message
   */
  def unhandled(message: Message): Unit = {
    system.log(s"Unhandled message: $message for actor ${self.path}")
  }

  /**
   * Called when actor fails
   */
  def onFailure(cause: Throwable): Unit = {
    system.log(s"Actor ${self.path} failed: ${cause.getMessage}")
  }

  /**
   * Internal message processing loop - runs in virtual thread
   */
  private[actors] def start(): Unit = {
    try {
      preStart()
      processLoop()
    } catch {
      case ex: Throwable =>
        onFailure(ex)
        throw ex
    } finally {
      try {
        postStop()
      } catch {
        case ex: Throwable =>
          system.log(s"Error in postStop for ${self.path}: ${ex.getMessage}")
      }
    }
  }

  /**
   * Main message processing loop
   */
  private def processLoop(): Unit = {
    while (!Thread.currentThread().isInterrupted) {
      try {
        mailbox.dequeue() match {
          case Some(envelope) =>
            processMessage(envelope)
          case None =>
            // Brief pause to avoid busy-waiting
            Thread.sleep(1)
        }
      } catch {
        case ex: InterruptedException =>
          // Exit cleanly on interrupt
          return
        case ex: Throwable =>
          onFailure(ex)
          // Continue processing after failure (restart strategy could go here)
      }
    }
  }

  /**
   * Process a single message
   */
  private def processMessage(envelope: Envelope): Unit = {
    val message = envelope.message

    // Handle control messages
    message match {
      case Shutdown =>
        return
      case GetReference =>
        envelope.sender.foreach(_ ! self)
        return
      case _ =>
    }

    // Try to handle the message
    if (receive.isDefinedAt(message)) {
      Try(receive.apply(message)) match {
        case Success(_) =>
        case Failure(ex: Throwable) =>
          throw new actors.exceptions.ActorProcessingException(
            s"Exception while processing message for actor ${self.path}", ex)
      }
    } else {
      unhandled(message)
    }
  }
}

/**
 * Control messages
 */
object Shutdown extends AnyRef
object GetReference extends AnyRef
