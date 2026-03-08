# Scala Actors Library

A modern, high-performance actor system implementation for Scala 3, leveraging Java 21 virtual threads for massive concurrency.

## Features

- **Lightweight Actors**: Each actor runs in a Java 21 virtual thread, enabling thousands of concurrent actors with minimal resource overhead
- **Hierarchical Actor Model**: True parent-child relationships with cascade shutdown
- **Rich Messaging API**: Tell (`!`), Ask (`?`), and Forward patterns
- **Actor Selection**: Path-based lookup using Akka-style paths (`akka://system/user/actor`)
- **Built-in Scheduling**: Delayed and periodic task execution
- **Configurable Mailboxes**: Bounded/unbounded queues with delivery strategies
- **Supervisor Hierarchy**: Create robust actor trees with automatic cascade lifecycle management

## Requirements

- **JDK 21+** (required for virtual threads)
- **Scala 3.3+**

## Quick Start

### Creating an Actor System

```scala
import actors.{Actor, ActorSystem, Props}

// Create an actor system
val system = ActorSystem("my-system")
```

### Defining an Actor

```scala
class MyActor extends Actor {
  // Called when actor starts
  override def preStart(): Unit = {
    println("Actor is starting...")
  }

  // Message handler - must be implemented
  override def receive: Receive = {
    case "hello" =>
      println("Hello received!")

    case message: String =>
      println(s"Received: $message")

    case _ =>
      unhandled(_)
  }

  // Called when actor stops
  override def postStop(): Unit = {
    println("Actor is stopping...")
  }
}
```

### Creating and Messaging Actors

```scala
// Create an actor
val actor = system.actorOf(Props(() => new MyActor()), "my-actor")

// Send a message (tell pattern - fire and forget)
actor ! "hello"

// Use the ask pattern (request-response)
import scala.concurrent.Await
import scala.concurrent.duration._

val response: Future[AnyRef] = actor ? "query"
// Note: Actor must respond back via sender for this to work
```

### Shutdown

```scala
// Graceful shutdown
system.shutdown()
```

## Core Concepts

### Actor Lifecycle

Every actor goes through these phases:

1. **Creation**: Actor is instantiated via `Props` factory
2. **Start**: Virtual thread is spawned, `preStart()` is called
3. **Running**: Message processing loop is active
4. **Stop**: `Shutdown` message received, loop exits, `postStop()` is called

### Message Types

Any reference type can be a message:

```scala
// Case classes (recommended)
case class Greet(name: String) extends AnyRef
case object Ping extends AnyRef

// Or simple strings for quick demos
actor ! "ping"
```

### Actor Context

From within an actor, access the `context` for system interactions:

```scala
class ParentActor extends Actor {
  override def receive: Receive = {
    case "create-child" =>
      // Create a child actor
      val child = context.actorOf(Props(() => new ChildActor()), "child")

    case "get-children" =>
      // Get all child actors
      val children = context.children()

    case "stop-child" =>
      // Stop an actor
      context.stop(child)

    case "find-actor" =>
      // Lookup actor by path
      context.actorSelection("/user/parent/child") match {
        case Some(actor) => actor ! "hello"
        case None => println("Actor not found")
      }
  }
}
```

## Advanced Features

### Actor Hierarchy

Actors can form hierarchical structures. When a parent is stopped, all children are automatically stopped:

```scala
// Create hierarchy
val parent = system.actorOf(Props(() => new ParentActor()), "parent")

// Inside parent actor:
val child = context.actorOf(Props(() => new ChildActor()), "child")

// Access parent reference
self.parent match {
  case Some(p) => println(s"Parent path: ${p.path}")
  case None => println("Root-level actor")
}

// Stopping parent cascades to children
system.stop(parent)  // Child is automatically stopped
```

### Bounded Mailbox

Create actors with bounded mailboxes for backpressure:

```scala
import actors.MailboxConfig

val boundedActor = system.actorOf(
  Props(() => new MyActor(), MailboxConfig(capacity = 100)),
  "bounded-actor"
)
```

Delivery strategies:
- `DeliveryStrategy.Unordered` - No ordering guarantees (default)
- `DeliveryStrategy.Ordered` - FIFO ordering
- `DeliveryStrategy.AtLeastOnce` - Guaranteed delivery with retries
- `DeliveryStrategy.AtMostOnce` - May drop messages when full

### Scheduling

**Delayed execution:**

```scala
// Execute after 1 second
system.schedule(
  delay = 1000,
  unit = TimeUnit.MILLISECONDS,
  task = () => println("Executed after 1 second!")
)
```

**Absolute time scheduling:**

```scala
// Execute at specific time
val futureTime = System.currentTimeMillis() + 60000  // 1 minute from now
system.scheduleAt(
  time = futureTime,
  unit = TimeUnit.MILLISECONDS,
  task = () => println("Executed at absolute time!")
)
```

### Actor Path Format

Actors are identified by Akka-style paths:

```
akka://systemName/user/actorName
akka://systemName/user/parent/child/grandchild
```

Use the selector to find actors by path:

```scala
system.selector("/user/parent/child") match {
  case Some(actor) => actor ! "hello"
  case None => println("Not found")
}
```

## Examples

The library includes comprehensive examples demonstrating various patterns:

### Basic Messaging

```scala
// Echo actor
class EchoActor extends Actor {
  override def receive: Receive = {
    case message: String =>
      println(s"[Echo] $message")
  }
}

val echo = system.actorOf(Props(() => new EchoActor()), "echo")
echo ! "Hello, World!"  // Output: [Echo] Hello, World!
```

### Request-Response Pattern

```scala
class MathService extends Actor {
  override def receive: Receive = {
    case Add(a, b) =>
      println(s"$a + $b = ${a + b}")
    case Multiply(a, b) =>
      println(s"$a \u00d7 $b = ${a \u00d7 b}")
  }
}

val mathService = system.actorOf(Props(() => new MathService()), "math")
mathService ! Add(10, 20)   // Output: 10 + 20 = 30
mathService ! Multiply(5, 6)  // Output: 5 \u00d7 6 = 30
```

### Supervisor Tree

```scala
class SupervisorActor extends Actor {
  override def receive: Receive = {
    case "create-workers" =>
      // Create worker hierarchy
      for (i <- 1 to 3) {
        context.actorOf(Props(() => new WorkerActor(i)), s"worker-$i")
      }
  }
}

val supervisor = system.actorOf(Props(() => new SupervisorActor()), "supervisor")
supervisor ! "create-workers"

// Stopping supervisor stops all workers automatically
system.stop(supervisor)
```

### Router Pattern (Round-Robin)

```scala
class RoundRobinRouter extends Actor {
  private var routers = List[ActorRef]()
  private var currentIndex = 0

  override def receive: Receive = {
    case RegisterRouter(ref) =>
      routers = ref :: routers

    case work: WorkItem =>
      val target = routers(currentIndex % routers.length)
      currentIndex += 1
      target ! work  // Distribute evenly
  }
}
```

### Function-Based Actors (Lambda Syntax)

```scala
// Quick actor from a function
val receive: Receive = {
  case message: String => println(s"Received: $message")
}
val simpleActor = system.actorOf(Props(receive), "simple")
```

## Running Examples

```bash
# Run all examples
sbt 'runMain actors.examples.ExamplesMain'

# Examples included:
# - BasicMessaging
# - RequestResponse
# - SupervisorTree
# - RouterPattern
# - ScheduledTasks
# - BackpressureDemo
```

## Testing

```bash
# Run all tests
sbt test

# Run tests with coverage
sbt coverage test
sbt coverageReport
```

## Architecture

### Key Components

| Component | Description |
|-----------|-------------|
| `ActorSystem` | Root system managing all actors |
| `Actor` | Base class for all actors |
| `ActorRef` | Immutable actor reference for messaging |
| `ActorContext` | Context for system interactions |
| `Props` | Actor factory configuration |
| `Mailbox` | Thread-safe message queue |
| `ActorPath` | Actor identification/lookup |

### Thread Model

Each actor runs in its own Java 21 virtual thread:

```scala
// Inside ActorSystem.createActor
Thread.startVirtualThread(new Runnable {
  override def run(): Unit = {
    actor.start()  // Actor message loop
  }
})
```

This enables massive concurrency with minimal resource usage - thousands of actors can run simultaneously without the overhead of OS threads.

## License

MIT License
