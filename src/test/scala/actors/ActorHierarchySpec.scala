package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ActorHierarchySpec extends AnyFunSpec with Matchers {

  describe("Actor Hierarchy") {
    it("should create child actors under parent") {
      val system = ActorSystem("test")
      val parent = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "parent")

      val child = system.actorOf(parent, Props(() => new Actor {
        def receive = { case _ => }
      }), "child")

      // Parent is at /user/parent, child is at /user/parent/child
      child.path.toString should endWith("/user/parent/child")
      system.selector("/user/parent/child") should be(defined)
      system.shutdown()
    }

    it("should track parent-child relationship") {
      val system = ActorSystem("test")
      val parent = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "parent")

      val child = system.actorOf(parent, Props(() => new Actor {
        def receive = { case _ => }
      }), "child")

      // Child should have parent reference
      child.parent shouldBe defined
      child.parent.get.path.toString should endWith("/user/parent")

      system.shutdown()
    }

    it("should cascade shutdown to children") {
      val system = ActorSystem("test")
      val parent = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "parent")

      val child = system.actorOf(parent, Props(() => new Actor {
        def receive = { case _ => }
      }), "child")

      system.stop(parent)

      system.selector("/user/parent/child") should be(empty)
      system.selector("/user/parent") should be(empty)
      system.shutdown()
    }

    it("should support multi-level hierarchy") {
      val system = ActorSystem("test")
      val grandparent = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "grandparent")

      val parent = system.actorOf(grandparent, Props(() => new Actor {
        def receive = { case _ => }
      }), "parent")

      val child = system.actorOf(parent, Props(() => new Actor {
        def receive = { case _ => }
      }), "child")

      child.path.toString should endWith("/user/grandparent/parent/child")
      system.stop(grandparent) // Should cascade to all
      system.shutdown()
    }

    it("should allow querying children") {
      val system = ActorSystem("test")
      val parent = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "parent")

      system.actorOf(parent, Props(() => new Actor {
        def receive = { case _ => }
      }), "child1")
      system.actorOf(parent, Props(() => new Actor {
        def receive = { case _ => }
      }), "child2")

      // Parent should see both children
      system.children(parent).size shouldBe 2

      system.shutdown()
    }

    it("should create actors under /user guardian") {
      val system = ActorSystem("test")
      val actor1 = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "actor1")
      val actor2 = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "actor2")

      // Both actors should be under /user
      actor1.path.toString should endWith("/user/actor1")
      actor2.path.toString should endWith("/user/actor2")

      // Both should have /user as parent
      actor1.parent.get.path.toString should endWith("/user")
      actor2.parent.get.path.toString should endWith("/user")

      system.shutdown()
    }

    it("should handle stopping actors with no children") {
      val system = ActorSystem("test")
      val actor = system.actorOf(Props(() => new Actor {
        def receive = { case _ => }
      }), "actor")

      // Should not throw
      system.stop(actor)
      system.selector("/user/actor") should be(empty)
      system.shutdown()
    }
  }
}
