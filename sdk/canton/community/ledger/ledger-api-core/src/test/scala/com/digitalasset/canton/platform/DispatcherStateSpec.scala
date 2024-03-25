// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.lf.data.Ref
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.pekkostreams.dispatcher.{Dispatcher, SubSource}
import io.grpc.StatusRuntimeException
import org.apache.pekko.stream.scaladsl.Source
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.{Failure, Success}

class DispatcherStateSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with PekkoBeforeAndAfterAll
    with BaseTest {
  private val className = classOf[DispatcherState].getSimpleName

  private val initialInitializationOffset =
    Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))

  private val nextOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdfe"))

  private val thirdOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdff"))

  s"$className.{startDispatcher, stopDispatcher}" should "handle correctly the Dispatcher lifecycle" in {
    for {
      _ <- Future.unit
      dispatcherState = new DispatcherState(Duration.Zero, loggerFactory)
      // Start the initial Dispatcher
      _ = dispatcherState.startDispatcher(initialInitializationOffset)

      // Assert running flag
      _ = dispatcherState.isRunning shouldBe true

      initialDispatcher = dispatcherState.getDispatcher

      // Stop the initial Dispatcher
      _ <- stopDispatcherAndAssertStreamsFinishedWithFailure(dispatcherState)

      // Assert running flag is false
      _ = dispatcherState.isRunning shouldBe false

      // Assert that the initial Dispatcher reference does not accept new subscriptions
      _ <- assertDispatcherDoesntAcceptNewSubscriptions(initialDispatcher)

      // Getting the Dispatcher while stopped throws
      _ = assertNotRunning(dispatcherState)

      // Start a new Dispatcher
      _ = dispatcherState.startDispatcher(nextOffset)

      // Try to start a new Dispatcher
      _ = intercept[IllegalStateException] {
        dispatcherState.startDispatcher(thirdOffset)
      }.getMessage shouldBe "Dispatcher startup triggered while an existing dispatcher is still active."

      anotherDispatcher = dispatcherState.getDispatcher
    } yield {
      // Assert that the dispatcher instances are different
      initialDispatcher should not be anotherDispatcher
    }
  }

  s"$className.shutdown" should "shutdown the DispatcherState" in {
    for {
      _ <- Future.unit
      dispatcherState = new DispatcherState(1.second, loggerFactory)
      // Start the initial Dispatcher
      _ = dispatcherState.startDispatcher(initialInitializationOffset)

      // Assert running flag
      _ = dispatcherState.isRunning shouldBe true
      initialDispatcher = dispatcherState.getDispatcher

      // Shutdown the state
      _ <- dispatcherState.shutdown()

      // Assert running flag is false
      _ = dispatcherState.isRunning shouldBe false

      // Assert that the initial Dispatcher reference does not accept new subscriptions
      _ <- assertDispatcherDoesntAcceptNewSubscriptions(initialDispatcher)

      // Getting the Dispatcher while shutdown
      _ = assertNotRunning(dispatcherState)

      // Start a new Dispatcher is not possible in the shutdown state
      _ = intercept[IllegalStateException] {
        dispatcherState.startDispatcher(nextOffset)
      }.getMessage shouldBe "Ledger API offset dispatcher state has already shut down."
    } yield succeed
  }

  s"$className.shutdown" should "work on not-running Dispatcher state" in {
    for {
      _ <- Future.unit
      // Start a new dispatcher state
      dispatcherState = new DispatcherState(Duration.Zero, loggerFactory)

      // Shutting down the state
      _ <- dispatcherState.shutdown()

      // Assert shutdown
      _ = assertNotRunning(dispatcherState)

      // Stopping the Dispatcher should be a no-op on a shutdown dispatcher
      _ <- dispatcherState.stopDispatcher()
    } yield succeed
  }

  private def assertNotRunning(dispatcherState: DispatcherState) =
    ErrorDetails.matches(
      e = intercept[StatusRuntimeException] { dispatcherState.getDispatcher },
      errorCode = CommonErrors.ServiceNotRunning,
    )

  private def assertDispatcherDoesntAcceptNewSubscriptions(
      initialDispatcher: Dispatcher[Offset]
  ): Future[Assertion] =
    initialDispatcher
      .startingAt(
        Offset.beforeBegin,
        SubSource.RangeSource((_, _) => Source.empty),
      )
      .run()
      .transform {
        case Failure(f) if f.getMessage == "Ledger API offset dispatcher: Dispatcher is closed" =>
          Success(succeed)
        case other => fail(s"Unexpected result: $other")
      }

  private def stopDispatcherAndAssertStreamsFinishedWithFailure(
      dispatcherState: DispatcherState
  ): Future[Unit] =
    for {
      _ <- Future.unit
      // Start a subscription
      runF = dispatcherState.getDispatcher
        .startingAt(Offset.beforeBegin, SubSource.RangeSource((_, _) => Source.empty))
        .run()
      // Stop the dispatcher
      _ <- dispatcherState.stopDispatcher()
      // Assert subscription correctly terminated with failure
      _ <- runF.transform {
        case Failure(e: StatusRuntimeException)
            if ErrorDetails.matches(e, CommonErrors.ServiceNotRunning) =>
          Success(())
        case Failure(other) =>
          fail(
            s"Expected a self-service error exception of ${CommonErrors.ServiceNotRunning.code} but got $other"
          )
        case Success(_) => fail("Expected a failure but got a Success instead")
      }
    } yield ()
}
