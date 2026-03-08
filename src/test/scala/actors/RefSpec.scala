package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RefSpec extends AnyFunSpec with Matchers {

  describe("Ref") {

    it("should set and get values") {
      val ref = new Ref[Int](42)
      ref.get() shouldBe 42

      ref.set(100)
      ref.get() shouldBe 100
    }

    it("should set and get string values") {
      val ref = new Ref[String]("initial")
      ref.get() shouldBe "initial"

      ref.set("updated")
      ref.get() shouldBe "updated"
    }

    it("should succeed with compareAndSet when value matches") {
      val ref = new Ref[Int](10)

      // Should succeed because current value is 10
      ref.compareAndSet(10, 20) shouldBe true
      ref.get() shouldBe 20

      // Should succeed because current value is now 20
      ref.compareAndSet(20, 30) shouldBe true
      ref.get() shouldBe 30
    }

    it("should fail with compareAndSet when value doesn't match") {
      val ref = new Ref[Int](10)

      // Should fail because current value is 10, not 5
      ref.compareAndSet(5, 20) shouldBe false
      ref.get() shouldBe 10  // Value unchanged

      // Should succeed now
      ref.compareAndSet(10, 20) shouldBe true
      ref.get() shouldBe 20
    }

    it("should support compareAndSet with null values") {
      val ref = new Ref[String]("initial")

      // Set to null
      ref.compareAndSet("initial", null) shouldBe true
      ref.get() shouldBe null

      // Set from null to another value
      ref.compareAndSet(null, "final") shouldBe true
      ref.get() shouldBe "final"
    }

    it("should support compareAndSet with case objects") {
      val ref = new Ref[Any](Shutdown)

      ref.compareAndSet(Shutdown, GetReference) shouldBe true
      ref.get() shouldBe GetReference
    }
  }
}
