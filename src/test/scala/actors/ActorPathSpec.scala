package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ActorPathSpec extends AnyFunSpec with Matchers {

  describe("ActorPath") {

    it("should create a valid path") {
      val path = ActorPath("test-system", "user/actor-name")
      path.systemName shouldBe "test-system"
      path.toString should include("actor-name")
    }

    it("should extract actor name from path") {
      val path = ActorPath("test", "user/my-actor")
      path.name shouldBe "my-actor"
    }

    it("should return parent path") {
      val path = ActorPath("test", "user/parent/child/grandchild")

      val grandchildParent = path.parent
      grandchildParent.isDefined shouldBe true
      grandchildParent.get.toString should endWith("/parent/child")

      val childPath = grandchildParent.get
      val childParent = childPath.parent
      childParent.get.toString should endWith("/parent")

      val parentPath = childParent.get
      val userParent = parentPath.parent
      userParent.get.toString should endWith("/user")

      val userParentPath = userParent.get
      userParentPath.parent shouldBe None
    }

    it("should create child path") {
      val parentPath = ActorPath("test", "user/parent")
      val childPath = parentPath.child("child-name")

      childPath.toString should endWith("/parent/child-name")
      childPath.name shouldBe "child-name"
      childPath.parent.get.toString should endWith("/parent")
    }

    it("should handle root-level paths") {
      val userPath = ActorPath("test", "user")
      userPath.parent shouldBe None
      userPath.name shouldBe "user"
    }

    it("should support deeply nested paths") {
      val level1 = ActorPath("system", "user/root")
      val level2 = level1.child("level2")
      val level3 = level2.child("level3")
      val level4 = level3.child("level4")
      val level5 = level4.child("level5")

      level5.toString should endWith("/user/root/level2/level3/level4/level5")
      level5.name shouldBe "level5"

      // Verify parent chain
      level5.parent.get.name shouldBe "level4"
      level5.parent.get.parent.get.name shouldBe "level3"
      level5.parent.get.parent.get.parent.get.name shouldBe "level2"
      level5.parent.get.parent.get.parent.get.parent.get.name shouldBe "root"
      level5.parent.get.parent.get.parent.get.parent.get.parent.get.name shouldBe "user"
      level5.parent.get.parent.get.parent.get.parent.get.parent.get.parent shouldBe None
    }

    it("should handle special characters in names") {
      val path1 = ActorPath("test", "user/actor-with-dash")
      path1.name shouldBe "actor-with-dash"

      val path2 = ActorPath("test", "user/actor_with_underscore")
      path2.name shouldBe "actor_with_underscore"

      val path3 = ActorPath("test", "user/actor.with.dots")
      path3.name shouldBe "actor.with.dots"
    }

    it("should have correct absolute path") {
      val path = ActorPath("my-system", "user/actor")
      path.absolute shouldBe "/user/actor"
    }
  }
}
