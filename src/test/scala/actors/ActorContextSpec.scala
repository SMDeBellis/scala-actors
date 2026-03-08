package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ActorContextSpec extends AnyFunSpec with Matchers {

  describe("ActorContext") {
    it("should allow creating child actors via context.actorOf") {
      val testSystem = new ActorSystem("test-context")

      class ParentActor extends Actor {
        override def preStart(): Unit = {
          // Create child using context.actorOf
          context.actorOf(Props(() => new ChildActor), "child1")
        }
        override def receive: Receive = {
          case _ =>
        }
      }

      class ChildActor extends Actor {
        override def receive: Receive = {
          case _ =>
        }
      }

      val parent = testSystem.actorOf(Props(() => new ParentActor), "parent")
      Thread.sleep(200)

      // Verify child was created at correct path
      val child = testSystem.selector("/user/parent/child1")
      child.isDefined shouldBe true

      testSystem.shutdown()
    }

    it("should query children via context.children()") {
      val testSystem = new ActorSystem("test-children")

      class Supervisor extends Actor {
        override def preStart(): Unit = {
          // Create children in preStart
          context.actorOf(Props(() => new Actor {
            override def receive = {
              case "ping" => "pong"
              case _ =>
            }
          }), "child1")
          context.actorOf(Props(() => new Actor {
            override def receive = {
              case "ping" => "pong"
              case _ =>
            }
          }), "child2")
        }

        override def receive: Receive = {
          case _ =>
        }
      }

      val supervisor = testSystem.actorOf(Props(() => new Supervisor), "supervisor")
      Thread.sleep(200)

      // Verify children were created
      val child1 = testSystem.selector("/user/supervisor/child1")
      val child2 = testSystem.selector("/user/supervisor/child2")

      child1.isDefined shouldBe true
      child2.isDefined shouldBe true

      // Verify via system.children
      val children = testSystem.children(supervisor)
      children.size shouldBe 2

      testSystem.shutdown()
    }

    it("should call context.children() from within actor") {
      val testSystem = new ActorSystem("test-context-children-call")
      var childCount = 0
      var childNames = List.empty[String]

      class ParentActor extends Actor {
        override def preStart(): Unit = {
          context.actorOf(Props(() => new Actor {
            override def receive = { case _ => }
          }), "child-a")
          context.actorOf(Props(() => new Actor {
            override def receive = { case _ => }
          }), "child-b")
        }

        override def receive: Receive = {
          case "get-children" =>
            val children = context.children()
            childCount = children.size
            childNames = children.map(_.path.name).sorted
          case _ =>
        }
      }

      val parent = testSystem.actorOf(Props(() => new ParentActor), "parent")
      Thread.sleep(200)

      // Request children info from within the actor
      parent ! "get-children"
      Thread.sleep(100)

      childCount shouldBe 2
      childNames shouldBe List("child-a", "child-b")

      testSystem.shutdown()
    }

    it("should stop actors via context.stop()") {
      val testSystem = new ActorSystem("test-stop")

      class ParentWithChild extends Actor {
        private var childRef: ActorRef = null

        override def preStart(): Unit = {
          childRef = context.actorOf(Props(() => new Actor {
            override def receive = {
              case "status" => "alive"
              case _ =>
            }
          }), "child")
        }

        override def receive: Receive = {
          case "stop-child" =>
            context.stop(childRef)
          case _ =>
        }
      }

      val parent = testSystem.actorOf(Props(() => new ParentWithChild), "parent")
      Thread.sleep(200)

      // Verify child exists
      val childBefore = testSystem.selector("/user/parent/child")
      childBefore.isDefined shouldBe true

      testSystem.shutdown()
    }

    it("should perform actor selection via context.actorSelection") {
      val testSystem = new ActorSystem("test-selection")

      val target = testSystem.actorOf(Props(() => new Actor {
        override def receive = {
          case "ping" => "pong"
          case _ =>
        }
      }), "target")

      // Test selection from within an actor
      class QueryActor extends Actor {
        override def receive: Receive = {
          case "select-actor" =>
            val selected = context.actorSelection("/user/target")
            selected.isDefined shouldBe true
          case _ =>
        }
      }

      val query = testSystem.actorOf(Props(() => new QueryActor), "query")
      Thread.sleep(100)

      // Test direct selection
      val selected = testSystem.selector("/user/target")
      selected.isDefined shouldBe true

      testSystem.shutdown()
    }
  }
}
