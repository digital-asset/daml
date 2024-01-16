// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class ResourceUtilTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  case class TestException(message: String) extends RuntimeException(message)

  private def mockResource: AutoCloseable = {
    val closeable = mock[AutoCloseable]
    doNothing.when(closeable).close()
    closeable
  }

  private def mockResourceThatThrowsExceptionWhenClosing: AutoCloseable = {
    val closeable = mock[AutoCloseable]
    doNothing.when(closeable).close()
    when(closeable.close()).thenThrow(TestException("Something happened when closing"))
    closeable
  }

  "ResourceUtil" when {
    "withResource" should {
      "return value from the function and close resource" in {
        val resource = mockResource
        val result = ResourceUtil.withResource(resource)(_ => "good")

        verify(resource, times(1)).close()
        result shouldBe "good"
      }

      "rethrow exception from function and still close resource" in {
        val resource = mockResource
        val exception = intercept[TestException](
          ResourceUtil.withResource(resource)(_ => throw TestException("Something happened"))
        )

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from closing" in {
        val resource = mockResourceThatThrowsExceptionWhenClosing
        val exception = intercept[TestException](ResourceUtil.withResource(resource)(_ => "good"))

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened when closing")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from function and add exception from closing to suppressed" in {
        val resource = mockResourceThatThrowsExceptionWhenClosing
        val exception = intercept[TestException](
          ResourceUtil.withResource(resource)(_ => throw TestException("Something happened"))
        )

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception
          .getSuppressed()(0) shouldBe TestException("Something happened when closing")
      }
    }

    "withResourceEither" should {
      "have the same behavior as withResources but return an Either with the result or exception" in {
        ResourceUtil.withResourceEither(mockResource)(_ => "good") shouldBe Right("good")
        ResourceUtil.withResourceEither(mockResource)(_ =>
          throw TestException("Something happened")
        ) shouldBe Left(TestException("Something happened"))
        ResourceUtil.withResourceEither(mockResourceThatThrowsExceptionWhenClosing)(_ =>
          "good"
        ) shouldBe Left(TestException("Something happened when closing"))
        ResourceUtil.withResourceEither(mockResourceThatThrowsExceptionWhenClosing)(_ =>
          throw TestException("Something happened")
        ) should matchPattern {
          case Left(e @ TestException("Something happened"))
              if e.getSuppressed()(0) == TestException("Something happened when closing") =>
        }
      }
    }

    "withResourceFuture" should {
      "return value from the function and close resource" in {
        val resource = mockResource
        val result = ResourceUtil.withResourceFuture(resource)(_ => Future("good")).futureValue

        verify(resource, times(1)).close()
        result shouldBe "good"
      }

      "rethrow exception from function and still close resource" in {
        val resource = mockResource
        val exception = ResourceUtil
          .withResourceFuture(resource)(_ => Future.failed(TestException("Something happened")))
          .failed
          .futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from outside of future and still close resource" in {
        val resource = mockResource
        val exception = ResourceUtil
          .withResourceFuture(resource)(_ => throw TestException("Something happened"))
          .failed
          .futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from closing" in {
        val resource = mockResourceThatThrowsExceptionWhenClosing
        val exception =
          ResourceUtil.withResourceFuture(resource)(_ => Future("good")).failed.futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened when closing")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from function and add exception from closing to suppressed" in {
        val resource = mockResourceThatThrowsExceptionWhenClosing
        val exception = ResourceUtil
          .withResourceFuture(resource)(_ => Future.failed(TestException("Something happened")))
          .failed
          .futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception
          .getSuppressed()(0) shouldBe TestException("Something happened when closing")
      }

    }

    "withResourceEitherT" should {
      "return value from the function and close resource" in {
        val resource = mockResource
        val resultRight = ResourceUtil
          .withResourceEitherT(resource)(_ => EitherT.rightT[Future, String]("good"))
          .futureValue
        val resultLeft = ResourceUtil
          .withResourceEitherT(resource)(_ => EitherT.leftT[Future, String]("good"))
          .value
          .futureValue
          .left
          .value

        verify(resource, times(2)).close()
        resultRight shouldBe "good"
        resultLeft shouldBe "good"
      }

      "rethrow exception from function and still close resource" in {
        val resource = mockResource
        val exception = ResourceUtil
          .withResourceEitherT(resource)(_ =>
            EitherT.liftF[Future, String, String](
              Future.failed(TestException("Something happened"))
            )
          )
          .value
          .failed
          .futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from outside of future and still close resource" in {
        val resource = mockResource
        val exception = ResourceUtil
          .withResourceEitherT(resource)(_ => throw TestException("Something happened"))
          .value
          .failed
          .futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from closing" in {
        val resource = mockResourceThatThrowsExceptionWhenClosing
        val exception = ResourceUtil
          .withResourceEitherT(resource)(_ => EitherT.rightT[Future, String]("good"))
          .value
          .failed
          .futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened when closing")
        exception.getSuppressed shouldBe empty
      }

      "rethrow exception from function and add exception from closing to suppressed" in {
        val resource = mockResourceThatThrowsExceptionWhenClosing
        val exception = ResourceUtil
          .withResourceEitherT(resource)(_ =>
            EitherT.liftF[Future, String, String](
              Future.failed(TestException("Something happened"))
            )
          )
          .value
          .failed
          .futureValue

        verify(resource, times(1)).close()
        exception shouldBe TestException("Something happened")
        exception
          .getSuppressed()(0) shouldBe TestException("Something happened when closing")
      }

    }
  }
}
