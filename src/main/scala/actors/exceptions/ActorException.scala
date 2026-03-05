package actors.exceptions

/**
 * Base exception type for actor-related errors
 */
class ActorException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/**
 * Thrown when an actor fails to start
 */
class ActorStartException(message: String, cause: Throwable = null) extends ActorException(message, cause)

/**
 * Thrown when an actor fails during message processing
 */
class ActorProcessingException(message: String, cause: Throwable = null) extends ActorException(message, cause)

/**
 * Thrown when an actor fails during shutdown
 */
class ActorShutdownException(message: String, cause: Throwable = null) extends ActorException(message, cause)

/**
 * Thrown when a message cannot be delivered
 */
class DeadLetterException(actorPath: String, message: String)
  extends ActorException(s"Dead letter for actor $actorPath: $message")

/**
 * Thrown when an actor is not found
 */
class ActorNotFoundException(path: String) extends ActorException(s"Actor not found: $path")

/**
 * Thrown when an actor is already stopped
 */
class ActorStoppedException(path: String) extends ActorException(s"Actor is stopped: $path")
