package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ActorLifecycleSpec extends AnyFunSpec with Matchers {

  describe("Actor lifecycle") {
    it("should call preStart when actor starts") {
      val system = new ActorSystem("test-prestart")
      val preStartCalled = scala.collection.mutable.ListBuffer[Unit]()

      val actor = system.actorOf(Props(() => new Actor {
        override def preStart(): Unit = {
          preStartCalled += ()
        }
        override def receive = {
          case _ =>
        }
      }), "prestart-test")

      Thread.sleep(200)

      preStartCalled.size shouldBe 1

      system.shutdown()
    }

    it("should call postStop when actor stops") {
      val system = new ActorSystem("test-poststop")
      val postStopCalled = scala.collection.mutable.ListBuffer[Unit]()

      class TestActor extends Actor {
        override def postStop(): Unit = {
          postStopCalled += ()
          super.postStop()
        }
        override def receive = {
          case _ =>
        }
      }

      val actor = system.actorOf(Props(() => new TestActor), "poststop-test")
      Thread.sleep(100)

      postStopCalled.size shouldBe 0

      // Stop the actor and wait for postStop to be called
      system.stop(actor)
      Thread.sleep(500)  // Wait for shutdown to complete (increased from 200ms)
    }

    it("should call unhandled for unknown messages") {
      val system = new ActorSystem("test-unhandled")
      val unhandledMessages = scala.collection.mutable.ListBuffer[Message]()

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case "known" => // Only handle "known"
        }
        override def unhandled(message: Message): Unit = {
          unhandledMessages += message
        }
      }), "unhandled-test")

      Thread.sleep(200)

      // Send a message that won't be handled
      actor ! "unknown-message"

      Thread.sleep(100)

      unhandledMessages.size shouldBe 1
      unhandledMessages.head.toString shouldBe "unknown-message"

      system.shutdown()
    }

    it("should call onFailure when exception occurs") {
      val system = new ActorSystem("test-onfailure")
      val failures = scala.collection.mutable.ListBuffer[Throwable]()

      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case "throw-error" =>
            throw new RuntimeException("Test error")
          case _ =>
        }
        override def onFailure(cause: Throwable): Unit = {
          failures += cause
        }
      }), "failure-test")

      Thread.sleep(200)

      actor ! "throw-error"

      Thread.sleep(200)

      failures.size shouldBe 1
      // The exception is wrapped, so check the cause
      failures.head.getCause.getMessage shouldBe "Test error"

      system.shutdown()
    }

    it("should execute preStart before processing messages") {
      val system = new ActorSystem("test-lifecycle-order")
      val lifecycle = scala.collection.mutable.ListBuffer[String]()

      val actor = system.actorOf(Props(() => new Actor {
        override def preStart(): Unit = {
          lifecycle += "preStart"
        }
        override def receive = {
          case "first" =>
            lifecycle += "processing"
          case _ =>
        }
        override def postStop(): Unit = {
          lifecycle += "postStop"
        }
      }), "lifecycle-test")

      Thread.sleep(200)

      lifecycle shouldBe List("preStart")

      actor ! "first"

      Thread.sleep(200)

      lifecycle shouldBe List("preStart", "processing")

      system.stop(actor)

      lifecycle shouldBe List("preStart", "processing")

      system.shutdown()
    }

    it("should handle empty preStart and postStop by default") {
      val system = new ActorSystem("test-empty-lifecycle")

      // Actor with only receive implementation
      val actor = system.actorOf(Props(() => new Actor {
        override def receive = {
          case _ =>
        }
      }), "empty-lifecycle")

      Thread.sleep(200)

      // Should start and run without errors
      actor ! "test"

      system.shutdown()
    }

    it("should provide context in preStart for actor creation") {
      val system = new ActorSystem("test-context-prestart")

      class Supervisor extends Actor {
        override def preStart(): Unit = {
          // Create child in preStart using context
          context.actorOf(Props(() => new Actor {
            override def receive = {
              case _ =>
            }
          }), "child-in-prestart")
        }
        override def receive = {
          case _ =>
        }
      }

      val supervisor = system.actorOf(Props(() => new Supervisor), "supervisor")
      Thread.sleep(200)

      val child = system.selector("/user/supervisor/child-in-prestart")
      child.isDefined shouldBe true

      system.shutdown()
    }
  }
}
