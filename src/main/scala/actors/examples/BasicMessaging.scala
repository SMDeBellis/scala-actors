package actors.examples

import actors.{Actor, ActorSystem, Message, Props, ActorRef, Receive}

/**
 * Demonstrates basic actor messaging patterns:
 * - Tell pattern (fire-and-forget)
 * - Actor creation and lifecycle
 * - Simple message processing
 */
object BasicMessaging {

  // Message types for basic messaging demo
  case class Greet(name: String) extends AnyRef
  case class Echo(message: String) extends AnyRef
  case class CounterIncrement(amount: Int) extends AnyRef
  case object CounterValue extends AnyRef
  case object CounterReset extends AnyRef

  /**
   * Simple echo actor - demonstrates basic message handling
   */
  class EchoActor extends Actor {

    override def preStart(): Unit = {
      system.log("EchoActor starting")
    }

    override def receive: Receive = {
      case Greet(name) =>
        println(s"[EchoActor] Hello, $name!")

      case Echo(message) =>
        println(s"[EchoActor] Echo: $message")

      case "ping" =>
        println("[EchoActor] Pong!")

      case _ =>
        unhandled(_)
    }

    override def postStop(): Unit = {
      system.log("EchoActor stopping")
    }
  }

  /**
   * Counter actor - demonstrates state management
   */
  class CounterActor(initialValue: Int = 0) extends Actor {
    private var count = initialValue

    override def preStart(): Unit = {
      system.log(s"CounterActor starting with value: $count")
    }

    override def receive: Receive = {
      case CounterIncrement(amount) =>
        count += amount
        println(s"[CounterActor] Incremented by $amount, new value: $count")

      case CounterReset =>
        count = 0
        println(s"[CounterActor] Reset to 0")

      case CounterValue =>
        println(s"[CounterActor] Current value: $count")

      case _ =>
        unhandled(_)
    }

    override def postStop(): Unit = {
      system.log(s"CounterActor stopping with final value: $count")
    }
  }

  /**
   * Function-based actor - demonstrates lambda syntax
   */
  def loggingActor(messagePrefix: String): Receive = {
    case msg: String =>
      println(s"[$messagePrefix] Received: $msg")
    case _ =>
      println(s"[$messagePrefix] Unhandled message")
  }

  /**
   * Run basic messaging example
   */
  def run(system: ActorSystem): Unit = {
    println("=== Basic Messaging Example ===")
    println()

    // Create echo actor using class
    println("1. Creating echo actor...")
    val echoActor = system.actorOf(Props(() => new EchoActor()), "echo-actor")

    Thread.sleep(50)

    // Send messages using tell pattern
    println("\n2. Sending messages to echo actor:")
    echoActor ! Greet("World")
    echoActor ! Greet("Scala Actors")
    echoActor ! Echo("Hello from tell pattern!")
    echoActor ! "ping"

    Thread.sleep(100)

    // Create counter actor
    println("\n3. Creating counter actor with initial value 10:")
    val counter = system.actorOf(Props(() => new CounterActor(10)), "counter")

    Thread.sleep(50)

    // Counter operations
    println("\n4. Counter operations:")
    counter ! CounterValue
    counter ! CounterIncrement(5)
    counter ! CounterIncrement(3)
    counter ! CounterReset
    counter ! CounterIncrement(100)
    counter ! CounterValue

    Thread.sleep(100)

    // Create function-based actor
    println("\n5. Creating function-based actor:")
    val logger = system.actorOf(Props(loggingActor("Logger")), "logger")
    logger ! "This is a log message"
    logger ! "Another message"

    Thread.sleep(100)

    println("\n=== Basic Messaging Example Complete ===")
  }
}
