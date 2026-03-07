package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SupervisorStrategySpec extends AnyFunSpec with Matchers {

  describe("SupervisorStrategy") {
    describe("enum values") {
      it("should have Restart") {
        SupervisorStrategy.Restart shouldBe SupervisorStrategy.Restart
      }

      it("should have Stop") {
        SupervisorStrategy.Stop shouldBe SupervisorStrategy.Stop
      }

      it("should have Resume") {
        SupervisorStrategy.Resume shouldBe SupervisorStrategy.Resume
      }

      it("should have Escalate") {
        SupervisorStrategy.Escalate shouldBe SupervisorStrategy.Escalate
      }
    }

    describe("equality") {
      it("should be equal to itself") {
        SupervisorStrategy.Restart shouldBe SupervisorStrategy.Restart
        SupervisorStrategy.Stop shouldBe SupervisorStrategy.Stop
      }

      it("should not be equal to different values") {
        SupervisorStrategy.Restart shouldNot be(SupervisorStrategy.Stop)
        SupervisorStrategy.Resume shouldNot be(SupervisorStrategy.Escalate)
      }
    }
  }

  describe("ActorSettings") {
    it("should have default values") {
      val settings = ActorSettings()
      settings.supervisorStrategy shouldBe SupervisorStrategy.Restart
      settings.restartMaxRetries shouldBe 3
      settings.restartBackoff shouldBe 1000
      settings.restartBackoffTimeUnit shouldBe TimeUnit.MILLISECONDS
    }

    it("should allow custom supervisor strategy") {
      val settings = ActorSettings(supervisorStrategy = SupervisorStrategy.Stop)
      settings.supervisorStrategy shouldBe SupervisorStrategy.Stop
    }

    it("should allow custom restart max retries") {
      val settings = ActorSettings(restartMaxRetries = 5)
      settings.restartMaxRetries shouldBe 5
    }

    it("should allow custom restart backoff") {
      val settings = ActorSettings(restartBackoff = 2000)
      settings.restartBackoff shouldBe 2000
    }

    it("should allow custom restart backoff time unit") {
      val settings = ActorSettings(restartBackoffTimeUnit = TimeUnit.SECONDS)
      settings.restartBackoffTimeUnit shouldBe TimeUnit.SECONDS
    }

    it("should allow all custom values") {
      val settings = ActorSettings(
        supervisorStrategy = SupervisorStrategy.Escalate,
        restartMaxRetries = 10,
        restartBackoff = 5000,
        restartBackoffTimeUnit = TimeUnit.MILLISECONDS
      )
      settings.supervisorStrategy shouldBe SupervisorStrategy.Escalate
      settings.restartMaxRetries shouldBe 10
      settings.restartBackoff shouldBe 5000
      settings.restartBackoffTimeUnit shouldBe TimeUnit.MILLISECONDS
    }

    it("should be immutable") {
      val settings = ActorSettings()
      val settingsCopy = ActorSettings(
        supervisorStrategy = settings.supervisorStrategy,
        restartMaxRetries = settings.restartMaxRetries,
        restartBackoff = settings.restartBackoff,
        restartBackoffTimeUnit = settings.restartBackoffTimeUnit
      )
      settings shouldBe settingsCopy
    }
  }
}
