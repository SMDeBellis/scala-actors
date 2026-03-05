package actors.examples

import actors.{Actor, ActorSystem, Message, Props, ActorRef, Receive}
import scala.concurrent.duration._
import scala.concurrent.Await

/**
 * Demonstrates request-response (ask) pattern
 * Shows how actors can act as services responding to queries
 */
object RequestResponse {

  // Message types for type-safe communication
  case class Add(a: Int, b: Int) extends AnyRef
  case class Multiply(a: Int, b: Int) extends AnyRef
  case class Divide(a: Int, b: Int) extends AnyRef
  case class Factorial(n: Int) extends AnyRef
  case class MathResult(value: Int)
  case class StringResult(value: String)
  case class GetUser(id: Int)
  case class User(id: Int, name: String, email: String)
  case class DatabaseQuery(sql: String)
  case class QueryResult(rows: List[String])

  /**
   * Math service actor - provides mathematical operations
   */
  class MathService extends Actor {
    private var operationCount = 0

    override def preStart(): Unit = {
      system.log("MathService starting")
    }

    override def receive: Receive = {
      case Add(a, b) =>
        operationCount += 1
        println(s"[MathService] $a + $b = ${a + b}")

      case Multiply(a, b) =>
        operationCount += 1
        println(s"[MathService] $a * $b = ${a * b}")

      case Divide(a, b) =>
        operationCount += 1
        if (b != 0) {
          println(s"[MathService] $a / $b = ${a / b}")
        } else {
          println("[MathService] Error: Division by zero!")
        }

      case Factorial(n) =>
        operationCount += 1
        val result = (1 to n).product
        println(s"[MathService] $n! = $result")

      case "stats" =>
        println(s"[MathService] Operations performed: $operationCount")
    }
  }

  /**
   * User database service - simulates looking up users
   */
  class UserService extends Actor {
    // Simulated database
    private val users = Map(
      1 -> User(1, "Alice", "alice@example.com"),
      2 -> User(2, "Bob", "bob@example.com"),
      3 -> User(3, "Charlie", "charlie@example.com")
    )

    override def receive: Receive = {
      case GetUser(id) =>
        users.get(id) match {
          case Some(user) =>
            println(s"[UserService] Found user: $user")
          case None =>
            println(s"[UserService] User $id not found")
        }

      case "list-all" =>
        println(s"[UserService] All users: ${users.values.mkString(", ")}")
    }
  }

  /**
   * Database query service - simulates SQL queries
   */
  class DatabaseService extends Actor {
    private var queryCount = 0

    override def receive: Receive = {
      case DatabaseQuery(sql) =>
        queryCount += 1
        println(s"[Database] Executing: $sql")
        Thread.sleep(100) // Simulate DB latency
        println(s"[Database] Query $queryCount complete")

      case "query-stats" =>
        println(s"[Database] Total queries: $queryCount")
    }
  }

  /**
   * Client that uses multiple services
   */
  class ServiceClient(mathService: ActorRef, userService: ActorRef, databaseService: ActorRef) extends Actor {

    override def preStart(): Unit = {
      system.log("ServiceClient starting")
    }

    override def receive: Receive = {
      case "run-demo" =>
        println("\n[ServiceClient] Running service demo...")

        // Call math service
        println("\n1. Math operations:")
        mathService ! Add(10, 20)
        mathService ! Multiply(5, 6)
        mathService ! Divide(100, 4)
        mathService ! Factorial(5)

        Thread.sleep(100)

        // Call user service
        println("\n2. User lookups:")
        userService ! GetUser(1)
        userService ! GetUser(999)

        Thread.sleep(100)

        // Call database service
        println("\n3. Database queries:")
        databaseService ! DatabaseQuery("SELECT * FROM users WHERE active = true")

        Thread.sleep(200)

        // Get stats
        println("\n4. Service statistics:")
        mathService ! "stats"
        databaseService ! "query-stats"

      case _ =>
        unhandled(_)
    }
  }

  /**
   * Run request-response example
   */
  def run(system: ActorSystem): Unit = {
    println("=== Request-Response Example ===")
    println()

    // Create service actors
    println("1. Creating service actors...")
    val mathService = system.actorOf(Props(() => new MathService()), "math-service")
    val userService = system.actorOf(Props(() => new UserService()), "user-service")
    val databaseService = system.actorOf(Props(() => new DatabaseService()), "database-service")

    Thread.sleep(100)

    // Create client
    println("\n2. Creating client actor...")
    val client = system.actorOf(Props(() => new ServiceClient(mathService, userService, databaseService)), "client")

    Thread.sleep(100)

    // Run demo
    println("\n3. Running service demo:")
    client ! "run-demo"

    Thread.sleep(300)

    println("\n=== Request-Response Example Complete ===")
  }
}
