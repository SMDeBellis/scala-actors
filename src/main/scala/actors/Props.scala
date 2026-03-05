package actors

/**
 * Configuration for creating actors
 * Thread-safe and immutable
 */
case class Props(
  actorFactory: () => Actor,
  mailboxConfig: MailboxConfig = MailboxConfig()
)

object Props {

  /**
   * Create Props from an actor instance factory
   */
  def apply(actorFactory: () => Actor): Props = {
    new Props(actorFactory, MailboxConfig())
  }

  /**
   * Create Props from a class (zero-argument constructor)
   */
  def apply[T <: Actor](clazz: Class[T]): Props = {
    apply(() => clazz.getDeclaredConstructor().newInstance())
  }

  /**
   * Create Props from a function (actor as a function)
   */
  def apply(behavior: PartialFunction[Message, Unit]): Props = {
    apply(() => new FunctionActor(behavior))
  }
}

/**
 * Simple actor implementation from a PartialFunction
 */
class FunctionActor(override val receive: PartialFunction[Message, Unit]) extends Actor {
  // Minimal implementation
}
