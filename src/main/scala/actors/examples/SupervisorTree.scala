package actors.examples

import actors.{Actor, ActorSystem, Message, Props, ActorRef, ActorPath, Receive}

/**
 * Demonstrates hierarchical actor relationships:
 * - Parent-child actor relationships
 * - Actor selection by path
 * - Actor lifecycle management
 * - Hierarchical messaging
 */
object SupervisorTree {

  // Messages for hierarchical communication
  case class CreateChild(name: String) extends AnyRef
  case class ChildResponse(message: String) extends AnyRef
  case class Broadcast(message: String) extends AnyRef
  case object ReportStatus extends AnyRef
  case class StatusReport(child: String, status: String) extends AnyRef
  case class SumRequest(values: List[Int]) extends AnyRef
  case class SumResult(total: Int) extends AnyRef

  /**
   * Supervisor actor that manages child workers
   */
  class SupervisorActor extends Actor {
    private var children = List[String]()

    override def preStart(): Unit = {
      system.log(s"Supervisor starting at ${self.path}")
    }

    override def receive: Receive = {
      case CreateChild(name) =>
        // Create child actor using context
        val child = context.actorOf(Props(() => new WorkerActor(name)), name)
        children = name :: children
        println(s"[Supervisor] Created child: $name at ${child.path}")

      case "list" =>
        println(s"[Supervisor] Children: ${children.mkString(", ")}")

      case Broadcast(msg) =>
        println(s"[Supervisor] Broadcasting: $msg")
        children.foreach { childName =>
          context.actorSelection(s"/$childName") match {
            case Some(child) => child ! msg
            case None => println(s"[Supervisor] Child not found: $childName")
          }
        }

      case ReportStatus =>
        children.foreach { childName =>
          context.actorSelection(s"/$childName") match {
            case Some(child) => child ! "status"
            case None => println(s"[Supervisor] Child not found: $childName")
          }
        }

      case StatusReport(child, status) =>
        println(s"  - [$child]: $status")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Worker actor that can be supervised
   */
  class WorkerActor(val name: String) extends Actor {
    private var tasksCompleted = 0
    private var totalWork = 0

    override def preStart(): Unit = {
      system.log(s"Worker $name starting")
    }

    override def receive: Receive = {
      case "status" =>
        println(s"    [Worker $name] Status: Tasks=$tasksCompleted, Work=$totalWork")

      case SumRequest(values) =>
        tasksCompleted += 1
        val sum = values.sum
        totalWork += sum
        println(s"  [Worker $name] Calculating sum of $values = $sum")

      case Broadcast(msg) =>
        println(s"  [Worker $name] Received broadcast: $msg")

      case "ping" =>
        println(s"  [Worker $name] Pong!")

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Root coordinator that manages multiple supervisors
   */
  class RootCoordinator extends Actor {
    override def preStart(): Unit = {
      system.log("RootCoordinator starting")
    }

    override def receive: Receive = {
      case "init" =>
        println("\n[Root] Creating supervisor hierarchy...")

        // Create supervisor actors
        val region1 = context.actorOf(Props(() => new SupervisorActor()), "region-1")
        val region2 = context.actorOf(Props(() => new SupervisorActor()), "region-2")

        println(s"[Root] Created supervisors:")
        println(s"  - ${region1.path}")
        println(s"  - ${region2.path}")

      case "create-workers" =>
        println("\n[Root] Creating workers under supervisors...")

        // Use actor selection to find supervisors and create workers
        context.actorSelection("/region-1") match {
          case Some(region1) =>
            region1 ! CreateChild("worker-1a")
            region1 ! CreateChild("worker-1b")
        }

        context.actorSelection("/region-2") match {
          case Some(region2) =>
            region2 ! CreateChild("worker-2a")
            region2 ! CreateChild("worker-2b")
        }

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Run supervisor tree example
   */
  def run(system: ActorSystem): Unit = {
    println("=== Supervisor Tree Example ===")
    println()

    // Create root coordinator
    println("1. Creating root coordinator...")
    val root = system.actorOf(Props(() => new RootCoordinator()), "root")

    Thread.sleep(100)

    // Initialize hierarchy
    println("\n2. Building actor hierarchy:")
    root ! "init"

    Thread.sleep(100)

    // Create workers
    println("\n3. Creating worker actors:")
    root ! "create-workers"

    Thread.sleep(100)

    // Demonstrate actor selection
    println("\n4. Direct worker communication (via actor selection):")
    system.selector("/worker-1a") match {
      case Some(worker) => worker ! "ping"
      case None => println("  Worker not found")
    }

    system.selector("/worker-1a") match {
      case Some(worker) => worker ! SumRequest(List(1, 2, 3, 4, 5))
      case None => println("  Worker-1a not found")
    }

    system.selector("/worker-2a") match {
      case Some(worker) => worker ! SumRequest(List(10, 20, 30))
      case None => println("  Worker-2a not found")
    }

    Thread.sleep(100)

    // Demonstrate hierarchical messaging
    println("\n5. Actor paths and hierarchy:")
    println("  Actor paths use hierarchical naming:")
    println("    akka://scala-actors/root")
    println("    akka://scala-actors/root/region-1")
    println("    akka://scala-actors/worker-1a")

    Thread.sleep(100)

    println("\n=== Supervisor Tree Example Complete ===")
  }
}
