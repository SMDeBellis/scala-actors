package actors.examples

import actors.{Actor, ActorSystem, Message, Props, ActorRef, Receive}
import scala.util.Random

/**
 * Demonstrates message routing patterns:
 * - Round-robin distribution
 * - Random selection
 * - Hash-based routing
 * - Consistent hashing
 */
object RouterPattern {

  // Messages for routing
  case class WorkItem(id: Int, data: String) extends AnyRef
  case class RouteRequest(message: String) extends AnyRef
  case class ForwardTo(target: String) extends AnyRef
  case class RegisterRouter(ref: ActorRef) extends AnyRef
  case class Broadcast(message: AnyRef) extends AnyRef

  /**
   * Round-robin router that distributes messages evenly
   */
  class RoundRobinRouter extends Actor {
    private var routers = List[ActorRef]()
    private var currentIndex = 0

    override def preStart(): Unit = {
      system.log("RoundRobinRouter starting")
    }

    override def receive: Receive = {
      case RegisterRouter(ref) =>
        routers = ref :: routers
        println(s"[RoundRobin] Registered router: ${ref.path} (total: ${routers.length})")

      case WorkItem(id, data) =>
        if (routers.nonEmpty) {
          val target = routers(currentIndex % routers.length)
          currentIndex += 1
          println(s"[RoundRobin] Routing item $id to ${target.path} (index: $currentIndex)")
          target ! WorkItem(id, data)
        } else {
          println(s"[RoundRobin] No routers available for item $id")
        }

      case "status" =>
        println(s"[RoundRobin] Active routers: ${routers.length}, Next index: $currentIndex")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Random router for load distribution
   */
  class RandomRouter extends Actor {
    private var routers = List[ActorRef]()
    private val random = new Random()

    override def preStart(): Unit = {
      system.log("RandomRouter starting")
    }

    override def receive: Receive = {
      case RegisterRouter(ref) =>
        routers = ref :: routers
        println(s"[Random] Registered router: ${ref.path}")

      case WorkItem(id, data) =>
        if (routers.nonEmpty) {
          val target = routers(random.nextInt(routers.length))
          println(s"[Random] Routing item $id to ${target.path}")
          target ! WorkItem(id, data)
        }

      case "list" =>
        println(s"[Random] Routers: ${routers.map(_.path.name).mkString(", ")}")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Hash-based router that uses message content to determine target
   */
  class HashRouter extends Actor {
    private var routers = List[ActorRef]()

    override def preStart(): Unit = {
      system.log("HashRouter starting")
    }

    override def receive: Receive = {
      case RegisterRouter(ref) =>
        routers = ref :: routers
        println(s"[Hash] Registered router: ${ref.path}")

      case WorkItem(id, data) =>
        if (routers.nonEmpty) {
          // Use work item ID for consistent routing
          val hash = id.abs % routers.length
          val target = routers(hash)
          println(s"[Hash] Routing item $id (hash: $hash) to ${target.path}")
          target ! WorkItem(id, data)
        }

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Fan-in/fan-out router
   */
  class FanOutRouter extends Actor {
    private var routers = List[ActorRef]()

    override def preStart(): Unit = {
      system.log("FanOutRouter starting")
    }

    override def receive: Receive = {
      case RegisterRouter(ref) =>
        routers = ref :: routers

      case Broadcast(message) =>
        println(s"[FanOut] Broadcasting to ${routers.length} routers")
        routers.foreach { router =>
          println(s"  - Sending to ${router.path}")
          router ! message
        }

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Worker that processes routed messages
   */
  class Worker(name: String) extends Actor {
    private var itemCount = 0

    override def preStart(): Unit = {
      system.log(s"Worker $name starting")
    }

    override def receive: Receive = {
      case WorkItem(id, data) =>
        itemCount += 1
        println(s"  [Worker $name] Processing item $id: $data (total: $itemCount)")

      case "count" =>
        println(s"  [Worker $name] Processed $itemCount items")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Run router pattern example
   */
  def run(system: ActorSystem): Unit = {
    println("=== Router Pattern Example ===")
    println()

    // Create workers
    println("1. Creating worker actors...")
    val worker1 = system.actorOf(Props(() => new Worker("worker-1")), "worker-1")
    val worker2 = system.actorOf(Props(() => new Worker("worker-2")), "worker-2")
    val worker3 = system.actorOf(Props(() => new Worker("worker-3")), "worker-3")

    Thread.sleep(100)

    // Round-robin routing
    println("\n2. Round-robin routing:")
    val roundRobin = system.actorOf(Props(() => new RoundRobinRouter()), "round-robin")
    roundRobin ! RegisterRouter(worker1)
    roundRobin ! RegisterRouter(worker2)
    roundRobin ! RegisterRouter(worker3)

    Thread.sleep(50)

    (1 to 6).foreach { i =>
      roundRobin ! WorkItem(i, s"data-$i")
    }

    Thread.sleep(100)

    // Random routing
    println("\n3. Random routing:")
    val randomRouter = system.actorOf(Props(() => new RandomRouter()), "random")
    randomRouter ! RegisterRouter(worker1)
    randomRouter ! RegisterRouter(worker2)

    Thread.sleep(50)

    (1 to 4).foreach { i =>
      randomRouter ! WorkItem(i, s"random-data-$i")
    }

    Thread.sleep(100)

    // Hash-based routing (consistent)
    println("\n4. Hash-based routing (same ID always goes to same worker):")
    val hashRouter = system.actorOf(Props(() => new HashRouter()), "hash")
    hashRouter ! RegisterRouter(worker1)
    hashRouter ! RegisterRouter(worker2)

    Thread.sleep(50)

    hashRouter ! WorkItem(10, "item-10")
    hashRouter ! WorkItem(20, "item-20")
    hashRouter ! WorkItem(10, "item-10-again")  // Same hash as first

    Thread.sleep(100)

    // Fan-out broadcasting
    println("\n5. Fan-out (broadcast) routing:")
    val fanOut = system.actorOf(Props(() => new FanOutRouter()), "fanout")
    fanOut ! RegisterRouter(worker1)
    fanOut ! RegisterRouter(worker2)
    fanOut ! RegisterRouter(worker3)

    Thread.sleep(50)

    fanOut ! Broadcast("System restart in 5 minutes")

    Thread.sleep(100)

    // Worker statistics
    println("\n6. Worker statistics:")
    worker1 ! "count"
    worker2 ! "count"
    worker3 ! "count"

    Thread.sleep(100)

    println("\n=== Router Pattern Example Complete ===")
  }
}
