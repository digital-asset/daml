// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.{Applicative, MonadThrow}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Failure, Success, Try}

class ResourceUtilTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  case class TestException(message: String) extends RuntimeException(message)

  private sealed trait CountCloseInvocations extends AutoCloseable {
    private val countRef: AtomicInteger = new AtomicInteger(0)
    def close(): Unit =
      countRef.incrementAndGet()
    def closeCount: Int = countRef.get()
  }

  private final class DoNothingResource extends CountCloseInvocations

  private final class ThrowOnCloseResource(message: String) extends CountCloseInvocations {
    override def close(): Unit = {
      super.close()
      throw TestException(message)
    }
  }

  private final class ResourceFactory(mkResource: () => AutoCloseable) {
    private val countCreations: AtomicInteger = new AtomicInteger(0)

    def create(): AutoCloseable = {
      countCreations.incrementAndGet()
      mkResource()
    }

    def counter: Int = countCreations.get()
  }

  "ResourceUtil" when {
    "withResource" should {
      "return value from the function and close resource" in {
        val resource = new DoNothingResource
        val result = ResourceUtil.withResource(resource)(_ => "good")

        resource.closeCount shouldBe 1
        result shouldBe "good"
      }

      "rethrow exception from function and still close resource" in {
        val resource = new DoNothingResource
        val exception = intercept[TestException](
          ResourceUtil.withResource(resource)(_ => throw TestException("Something happened"))
        )

        resource.closeCount shouldBe 1
        exception shouldBe TestException("Something happened")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from closing" in {
        val msg = "rethrow-exception message"
        val resource = new ThrowOnCloseResource(msg)
        val exception = intercept[TestException](ResourceUtil.withResource(resource)(_ => "good"))

        resource.closeCount shouldBe 1
        exception shouldBe TestException(msg)
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from function and add exception from closing to suppressed" in {
        val closeMsg = "rethrow-exception-message-from-closing"
        val resource = new ThrowOnCloseResource(closeMsg)
        val runMsg = "Something happened"
        val exception = intercept[TestException](
          ResourceUtil.withResource(resource)(_ => throw TestException(runMsg))
        )

        resource.closeCount shouldBe 1
        exception shouldBe TestException(runMsg)
        exception
          .getSuppressed()(0) shouldBe TestException(closeMsg)
      }

      "tolerate the same exception being thrown by the close and the run method" in {
        val ex = new TestException("test")
        val resource = new CountCloseInvocations {
          override def close(): Unit = {
            super.close()
            throw ex
          }
        }
        val exception = intercept[TestException](
          ResourceUtil.withResource(resource)(_ => throw ex)
        )
        resource.closeCount shouldBe 1
        exception shouldBe ex
      }

      "create a resource exactly once" in {
        val factory = new ResourceFactory(() => new DoNothingResource)
        ResourceUtil.withResource(factory.create())(_ => ())
        factory.counter shouldBe 1
      }

      "rethrow exceptions during resource creation" in {
        val ex = TestException("Resource construction failed")
        val exception = Try(ResourceUtil.withResource((throw ex): AutoCloseable)(_ => ()))
        exception shouldBe Failure(ex)
      }
    }

    "withResourceEither" should {
      "have the same behavior as withResource but return an Either with the result or exception" in {
        ResourceUtil.withResourceEither(new DoNothingResource)(_ => "good") shouldBe Right("good")

        val msg = "Something happened"
        ResourceUtil.withResourceEither(new DoNothingResource)(_ =>
          throw TestException(msg)
        ) shouldBe Left(TestException(msg))

        val closeMsg = "Something happened when closing"
        ResourceUtil.withResourceEither(new ThrowOnCloseResource(closeMsg))(_ =>
          "good"
        ) shouldBe Left(TestException(closeMsg))

        ResourceUtil.withResourceEither(new ThrowOnCloseResource(closeMsg))(_ =>
          throw TestException(msg)
        ) should matchPattern {
          case Left(e @ TestException(`msg`)) if e.getSuppressed()(0) == TestException(closeMsg) =>
        }
      }

      "catch exceptions during resource construction" in {
        val ex = TestException("Resource construction failed")
        val exception = ResourceUtil.withResourceEither((throw ex): AutoCloseable)(_ => ())
        exception shouldBe Left(ex)
      }

      "create a resource exactly once" in {
        val factory = new ResourceFactory(() => new DoNothingResource)
        ResourceUtil.withResourceEither(factory.create())(_ => ())
        factory.counter shouldBe 1
      }
    }

    def withResourceM[F[_], C[_], S](
        fixture: ThereafterTest.Fixture[F, C, S]
    )(implicit F: Thereafter.Aux[F, C, S], M: MonadThrow[F]): Unit = {
      "return value from the function and close resource" in {

        forAll(fixture.contents) { content =>
          val resource = new DoNothingResource
          val result = fixture.await(ResourceUtil.withResourceM(resource) { _ =>
            fixture.fromContent(content)
          })
          resource.closeCount shouldBe 1
          result shouldBe content
        }
      }

      "rethrow exception from outside of body and still close resource" in {
        val resource = new DoNothingResource
        val ex = TestException("Something happened")
        val result = fixture.await(ResourceUtil.withResourceM[F](resource)(_ => throw ex))
        Try(fixture.theContent(result)) shouldBe Failure(ex)
      }

      "rethrow exception from closing" in {
        val closeMsg = "Something happened when closing"
        val resource = new ThrowOnCloseResource(closeMsg)
        val result =
          fixture.await(ResourceUtil.withResourceM(resource)(_ => fixture.fromTry(Success("good"))))

        resource.closeCount shouldBe 1
        Try(fixture.theContent(result)) shouldBe Failure(TestException(closeMsg))
      }

      "rethrow exception from function and add exception from closing to suppressed" in {
        val closeMsg = "Something happened when closing"
        val resource = new ThrowOnCloseResource(closeMsg)
        val runEx = TestException("Something happened")
        val exception = fixture.await(
          ResourceUtil.withResourceM(resource)(_ => fixture.fromTry[String](Failure(runEx)))
        )

        resource.closeCount shouldBe 1
        Try(fixture.theContent(exception)) shouldBe Failure(runEx)
        runEx.getSuppressed()(0) shouldBe TestException(closeMsg)
      }

      "create a resource exactly once" in {
        val factory = new ResourceFactory(() => new DoNothingResource)
        fixture.await(
          ResourceUtil.withResourceM(factory.create())(_ => fixture.fromTry(Success(())))
        )
        factory.counter shouldBe 1
      }

      "rethrow exceptions during resource creation" in {
        val ex = TestException("Resource construction failed")
        val exception = fixture.await(
          ResourceUtil.withResourceM((throw ex): AutoCloseable)(_ => fixture.fromTry(Success(())))
        )
        exception shouldBe Failure(ex)
      }
    }

    "withResourceM" when {
      implicit def appTryUnlessShutdown = Applicative[Try].compose[UnlessShutdown]

      "used with Future" should {
        behave like withResourceM(FutureThereafterTest.fixture)
      }

      "used with FutureUnlessShutdown" should {
        behave like withResourceM(FutureUnlessShutdownThereafterTest.fixture)
      }

      "used with EitherT[Future]" should {
        behave like withResourceM(
          EitherTThereafterTest.fixture(FutureThereafterTest.fixture, NonEmpty(Seq, 1, 2))
        )
      }

      "used with EitherT[FutureUnlessShutdown]" should {
        behave like withResourceM(
          EitherTThereafterTest.fixture(
            FutureUnlessShutdownThereafterTest.fixture,
            NonEmpty(Seq, 1, 2),
          )
        )
      }

      "used with OptionT[Future]" should {
        behave like withResourceM(
          OptionTThereafterTest.fixture(FutureThereafterTest.fixture)
        )
      }

      "used with OptionT[FutureUnlessShutdown]" should {
        behave like withResourceM(
          OptionTThereafterTest.fixture(FutureUnlessShutdownThereafterTest.fixture)
        )
      }
    }
  }
}
