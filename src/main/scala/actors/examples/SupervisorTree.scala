package actors.examples

import actors.{Actor, ActorSystem, Message, Props, ActorRef, ActorPath, Receive}

/**
 * Demonstrates hierarchical actor relationships:
 * - Parent-child actor relationships
 * - Actor selection by path
 * - Actor lifecycle management
 * - Cascade shutdown
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
  case object GetChildren extends AnyRef
  case class ChildrenList(children: List[String]) extends AnyRef
  case object GetParent extends AnyRef
  case class ParentPath(path: String) extends AnyRef

  /**
   * Supervisor actor that manages child workers
   */
  class SupervisorActor extends Actor {
    private var tasksCompleted = 0

    override def preStart(): Unit = {
      system.log(s"Supervisor starting at ${self.path}")
    }

    override def receive: Receive = {
      case CreateChild(name) =>
        // Create child actor using context - child is created at self.path/child
        val child = context.actorOf(Props(() => new WorkerActor(name)), name)
        println(s"  [Supervisor] Created child: $name at ${child.path}")
        println(s"    Child's parent: ${child.parent.map(_.path.toString).getOrElse("none")}")

      case GetChildren =>
        // Use context.children() to query child actors - no manual tracking needed!
        val childRefs = context.children()
        val childPaths = childRefs.map(_.path.toString).sorted
        println(s"  [Supervisor] Children (${childRefs.size}): ${childPaths.mkString(", ")}")
        childRefs.foreach { child => child ! ReportStatus }

      case GetParent =>
        // Demonstrate parent reference access
        self.parent match {
          case Some(parent) =>
            println(s"  [Supervisor] My parent is: ${parent.path}")
          case None =>
            println(s"  [Supervisor] I have no parent (root-level actor)")
        }

      case Broadcast(msg) =>
        println(s"  [Supervisor] Broadcasting: $msg to children")
        context.children().foreach { child =>
          child ! Broadcast(msg)
        }

      case ReportStatus =>
        println(s"    [Supervisor ${self.path.name}] Status: tasks=$tasksCompleted, children=${context.children().size}")

      case "stop-region" =>
        println(s"  [Supervisor] Stopping region ${self.path.name} and all children...")
        // Cascade shutdown: stopping this supervisor will stop all workers
        context.stop(self)

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
        println(s"      [Worker $name] Status: Tasks=$tasksCompleted, Work=$totalWork")

      case SumRequest(values) =>
        tasksCompleted += 1
        val sum = values.sum
        totalWork += sum
        println(s"    [Worker $name] Calculating sum of $values = $sum")

      case Broadcast(msg) =>
        println(s"      [Worker $name] Received broadcast: $msg")

      case "ping" =>
        println(s"      [Worker $name] Pong!")

      case GetParent =>
        self.parent match {
          case Some(parent) =>
            println(s"      [Worker $name] My parent is: ${parent.path}")
          case None =>
            println(s"      [Worker $name] I have no parent")
        }

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

        // Create supervisor actors as children of root coordinator
        val region1 = context.actorOf(Props(() => new SupervisorActor()), "region-1")
        val region2 = context.actorOf(Props(() => new SupervisorActor()), "region-2")

        println(s"[Root] Created supervisors:")
        println(s"  - ${region1.path}")
        println(s"  - ${region2.path}")
        println(s"[Root] Both supervisors have parent: ${region1.parent.map(_.path.toString)}")

      case "create-workers" =>
        println("\n[Root] Creating workers under supervisors...")

        // Use actor selection with correct hierarchical paths
        context.actorSelection("/user/root/region-1") match {
          case Some(region1) =>
            region1 ! CreateChild("worker-1a")
            region1 ! CreateChild("worker-1b")
        }

        context.actorSelection("/user/root/region-2") match {
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

    // Demonstrate correct actor selection with hierarchical paths
    println("\n4. Direct worker communication (via hierarchical path selection):")
    system.selector("/user/root/region-1/worker-1a") match {
      case Some(worker) =>
        println("  Found worker-1a at: " + worker.path)
        worker ! "ping"
      case None => println("  Worker-1a not found")
    }

    system.selector("/user/root/region-1/worker-1a") match {
      case Some(worker) => worker ! SumRequest(List(1, 2, 3, 4, 5))
      case None => println("  Worker-1a not found")
    }

    system.selector("/user/root/region-2/worker-2a") match {
      case Some(worker) => worker ! SumRequest(List(10, 20, 30))
      case None => println("  Worker-2a not found")
    }

    Thread.sleep(100)

    // Demonstrate querying children using context.children()
    println("\n5. Querying children (using context.children()):")
    system.selector("/user/root/region-1") match {
      case Some(region1) => region1 ! GetChildren
      case None => println("  Region-1 not found")
    }

    Thread.sleep(100)

    // Demonstrate parent reference
    println("\n6. Accessing parent references:")
    system.selector("/user/root/region-1/worker-1a") match {
      case Some(worker) => worker ! GetParent
      case None => println("  Worker-1a not found")
    }
    system.selector("/user/root/region-1") match {
      case Some(region) => region ! GetParent
      case None => println("  Region-1 not found")
    }

    Thread.sleep(100)

    // Demonstrate cascade shutdown - THE KEY FEATURE!
    println("\n7. Cascade shutdown (stopping region-1 stops its workers):")
    println("  Before shutdown:")
    system.selector("/user/root/region-1") match {
      case Some(region1) =>
        val children = system.children(region1)
        println(s"    Region-1 has ${children.size} children: ${children.map(_.path.name).mkString(", ")}")
      case None => println("  Region-1 not found")
    }
    system.selector("/user/root/region-1/worker-1a") match {
      case Some(_) => println("    Worker-1a: alive")
      case None => println("    Worker-1a: not found")
    }

    // Stop region-1 - this cascades to worker-1a and worker-1b!
    system.selector("/user/root/region-1") match {
      case Some(region1) => system.stop(region1)
      case None => println("  Region-1 not found")
    }

    Thread.sleep(200)

    println("  After shutdown:")
    system.selector("/user/root/region-1") match {
      case Some(_) => println("    Region-1: alive")
      case None => println("    Region-1: stopped ✓")
    }
    system.selector("/user/root/region-1/worker-1a") match {
      case Some(_) => println("    Worker-1a: alive")
      case None => println("    Worker-1a: stopped (cascaded) ✓")
    }
    system.selector("/user/root/region-1/worker-1b") match {
      case Some(_) => println("    Worker-1b: alive")
      case None => println("    Worker-1b: stopped (cascaded) ✓")
    }

    Thread.sleep(100)

    // Show full hierarchy
    println("\n8. Complete actor hierarchy:")
    println("  akka://scala-actors/")
    println("    └── user/")
    println("        └── root/                (RootCoordinator)")
    println("            ├── region-1/        (stopped)")
    println("            │   ├── worker-1a/   (stopped)")
    println("            │   └── worker-1b/   (stopped)")
    println("            └── region-2/        (active)")
    println("                ├── worker-2a/   (active)")
    println("                └── worker-2b/   (active)")

    Thread.sleep(100)

    println("\n=== Supervisor Tree Example Complete ===")
  }
}
