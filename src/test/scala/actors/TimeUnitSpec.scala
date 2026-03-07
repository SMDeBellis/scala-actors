package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import java.util.concurrent.TimeUnit as JavaTimeUnit

class TimeUnitSpec extends AnyFunSpec with Matchers {

  describe("TimeUnit") {
    describe("enum values") {
      it("should have NANOSECONDS") {
        TimeUnit.NANOSECONDS shouldBe TimeUnit.NANOSECONDS
      }

      it("should have MICROSECONDS") {
        TimeUnit.MICROSECONDS shouldBe TimeUnit.MICROSECONDS
      }

      it("should have MILLISECONDS") {
        TimeUnit.MILLISECONDS shouldBe TimeUnit.MILLISECONDS
      }

      it("should have SECONDS") {
        TimeUnit.SECONDS shouldBe TimeUnit.SECONDS
      }

      it("should have MINUTES") {
        TimeUnit.MINUTES shouldBe TimeUnit.MINUTES
      }

      it("should have HOURS") {
        TimeUnit.HOURS shouldBe TimeUnit.HOURS
      }

      it("should have DAYS") {
        TimeUnit.DAYS shouldBe TimeUnit.DAYS
      }
    }

    describe("toJava conversion") {
      it("should convert NANOSECONDS to Java NANOSECONDS") {
        TimeUnit.NANOSECONDS.toJava shouldBe JavaTimeUnit.NANOSECONDS
      }

      it("should convert MICROSECONDS to Java MICROSECONDS") {
        TimeUnit.MICROSECONDS.toJava shouldBe JavaTimeUnit.MICROSECONDS
      }

      it("should convert MILLISECONDS to Java MILLISECONDS") {
        TimeUnit.MILLISECONDS.toJava shouldBe JavaTimeUnit.MILLISECONDS
      }

      it("should convert SECONDS to Java SECONDS") {
        TimeUnit.SECONDS.toJava shouldBe JavaTimeUnit.SECONDS
      }

      it("should convert MINUTES to Java MINUTES") {
        TimeUnit.MINUTES.toJava shouldBe JavaTimeUnit.MINUTES
      }

      it("should convert HOURS to Java HOURS") {
        TimeUnit.HOURS.toJava shouldBe JavaTimeUnit.HOURS
      }

      it("should convert DAYS to Java DAYS") {
        TimeUnit.DAYS.toJava shouldBe JavaTimeUnit.DAYS
      }
    }

    describe("equality") {
      it("should be equal to itself") {
        TimeUnit.SECONDS shouldBe TimeUnit.SECONDS
        TimeUnit.MILLISECONDS shouldBe TimeUnit.MILLISECONDS
      }

      it("should not be equal to different values") {
        TimeUnit.SECONDS shouldNot be(TimeUnit.MILLISECONDS)
        TimeUnit.HOURS shouldNot be(TimeUnit.MINUTES)
      }
    }
  }
}
