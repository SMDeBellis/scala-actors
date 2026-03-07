package actors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import actors.exceptions._
import java.io.IOException

/**
 * Tests for actor exception types
 */
class ExceptionsSpec extends AnyFunSpec with Matchers {

  describe("ActorException") {
    it("should create with message") {
      val exception = new ActorException("test message")
      exception.getMessage shouldBe "test message"
      exception.getCause shouldBe null
    }

    it("should create with message and cause") {
      val cause = new RuntimeException("original error")
      val exception = new ActorException("test message", cause)
      exception.getMessage shouldBe "test message"
      exception.getCause shouldBe cause
    }
  }

  describe("ActorStartException") {
    it("should create with message") {
      val exception = new ActorStartException("failed to start")
      exception.getMessage shouldBe "failed to start"
      exception.isInstanceOf[ActorException] shouldBe true
    }

    it("should create with message and cause") {
      val cause = new RuntimeException("initialization failed")
      val exception = new ActorStartException("failed to start", cause)
      exception.getMessage shouldBe "failed to start"
      exception.getCause shouldBe cause
    }

    it("should be a subtype of ActorException") {
      val exception = new ActorStartException("test")
      exception.isInstanceOf[ActorException] shouldBe true
    }
  }

  describe("ActorProcessingException") {
    it("should create with message") {
      val exception = new ActorProcessingException("processing failed")
      exception.getMessage shouldBe "processing failed"
      exception.isInstanceOf[ActorException] shouldBe true
    }

    it("should create with message and cause") {
      val cause = new IllegalArgumentException("invalid argument")
      val exception = new ActorProcessingException("processing failed", cause)
      exception.getMessage shouldBe "processing failed"
      exception.getCause shouldBe cause
    }

    it("should be a subtype of ActorException") {
      val exception = new ActorProcessingException("test")
      exception.isInstanceOf[ActorException] shouldBe true
    }
  }

  describe("ActorShutdownException") {
    it("should create with message") {
      val exception = new ActorShutdownException("shutdown failed")
      exception.getMessage shouldBe "shutdown failed"
      exception.isInstanceOf[ActorException] shouldBe true
    }

    it("should create with message and cause") {
      val cause = new InterruptedException("interrupted")
      val exception = new ActorShutdownException("shutdown failed", cause)
      exception.getMessage shouldBe "shutdown failed"
      exception.getCause shouldBe cause
    }

    it("should be a subtype of ActorException") {
      val exception = new ActorShutdownException("test")
      exception.isInstanceOf[ActorException] shouldBe true
    }
  }

  describe("DeadLetterException") {
    it("should create with actor path and message") {
      val path = "/user/actor"
      val msg = "message content"
      val exception = new DeadLetterException(path, msg)
      exception.getMessage shouldBe s"Dead letter for actor $path: $msg"
      exception.isInstanceOf[ActorException] shouldBe true
    }

    it("should format full path in message") {
      val path = "akka://system/user/test"
      val msg = "Ping"
      val exception = new DeadLetterException(path, msg)
      exception.getMessage shouldBe "Dead letter for actor akka://system/user/test: Ping"
    }

    it("should be a subtype of ActorException") {
      val exception = new DeadLetterException("/test", "msg")
      exception.isInstanceOf[ActorException] shouldBe true
    }
  }

  describe("ActorNotFoundException") {
    it("should create with path") {
      val path = "/user/nonexistent"
      val exception = new ActorNotFoundException(path)
      exception.getMessage shouldBe s"Actor not found: $path"
      exception.isInstanceOf[ActorException] shouldBe true
    }

    it("should format path in message") {
      val path = "akka://system/user/missing"
      val exception = new ActorNotFoundException(path)
      exception.getMessage shouldBe "Actor not found: akka://system/user/missing"
    }

    it("should be a subtype of ActorException") {
      val exception = new ActorNotFoundException("/test")
      exception.isInstanceOf[ActorException] shouldBe true
    }
  }

  describe("ActorStoppedException") {
    it("should create with path") {
      val path = "/user/stopped"
      val exception = new ActorStoppedException(path)
      exception.getMessage shouldBe s"Actor is stopped: $path"
      exception.isInstanceOf[ActorException] shouldBe true
    }

    it("should format path in message") {
      val path = "akka://system/user/halted"
      val exception = new ActorStoppedException(path)
      exception.getMessage shouldBe "Actor is stopped: akka://system/user/halted"
    }

    it("should be a subtype of ActorException") {
      val exception = new ActorStoppedException("/test")
      exception.isInstanceOf[ActorException] shouldBe true
    }
  }

  describe("Exception hierarchy") {
    it("should have proper inheritance") {
      val startEx = new ActorStartException("test")
      val processEx = new ActorProcessingException("test")
      val shutdownEx = new ActorShutdownException("test")
      val deadLetterEx = new DeadLetterException("/test", "msg")
      val notFoundEx = new ActorNotFoundException("/test")
      val stoppedEx = new ActorStoppedException("/test")

      // All should be ActorExceptions
      startEx.isInstanceOf[ActorException] shouldBe true
      processEx.isInstanceOf[ActorException] shouldBe true
      shutdownEx.isInstanceOf[ActorException] shouldBe true
      deadLetterEx.isInstanceOf[ActorException] shouldBe true
      notFoundEx.isInstanceOf[ActorException] shouldBe true
      stoppedEx.isInstanceOf[ActorException] shouldBe true

      // All should be RuntimeExceptions
      startEx.isInstanceOf[RuntimeException] shouldBe true
      processEx.isInstanceOf[RuntimeException] shouldBe true
      shutdownEx.isInstanceOf[RuntimeException] shouldBe true
      deadLetterEx.isInstanceOf[RuntimeException] shouldBe true
      notFoundEx.isInstanceOf[RuntimeException] shouldBe true
      stoppedEx.isInstanceOf[RuntimeException] shouldBe true
    }

    it("should preserve exception chain") {
      val original = new IOException("original")
      val wrapped = new ActorProcessingException("wrapped", original)

      wrapped.getCause shouldBe original
      wrapped.getCause.getClass.getName shouldBe "java.io.IOException"
    }
  }
}
