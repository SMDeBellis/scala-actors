package actors.examples

import actors.ActorSystem
import scala.concurrent.duration._

/**
 * Main entry point that runs all examples
 * Demonstrates the full range of scala-actors functionality
 */
object ExamplesMain {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("  Scala Actors Library - Examples Suite")
    println("=" * 60)
    println()

    // Create actor system
    val system = ActorSystem("scala-actors")

    try {
      // Run examples sequentially
      runExample("Basic Messaging")(BasicMessaging.run(system))
      runExample("Request-Response")(RequestResponse.run(system))
      runExample("Supervisor Tree")(SupervisorTree.run(system))
      runExample("Router Pattern")(RouterPattern.run(system))
      runExample("Scheduled Tasks")(ScheduledTasks.run(system))
      runExample("Backpressure Demo")(BackpressureDemo.run(system))

    } finally {
      // Graceful shutdown
      println()
      println("=" * 60)
      println("  Shutting down actor system...")
      system.shutdown()
      println("  All actors stopped.")
      println("=" * 60)
    }
  }

  /**
   * Helper to run examples with consistent formatting
   */
  private def runExample(name: String)(example: => Unit): Unit = {
    println()
    println("-" * 60)
    println(s"  Running: $name")
    println("-" * 60)
    example
    println()
    println(s"  ✓ $name completed")
    println()
  }
}
