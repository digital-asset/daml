// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import akka.stream.scaladsl.Source
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.{Dispatcher, SubSource}
import com.daml.platform.store.backend.ParameterStorageBackend
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class DispatcherStateSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with AkkaBeforeAndAfterAll {
  private val loggingContext = LoggingContext.ForTesting
  private val className = classOf[DispatcherState].getSimpleName

  private val initialLedgerEnd = ParameterStorageBackend
    .LedgerEnd(
      lastOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef")),
      lastEventSeqId = 1337L,
      lastStringInterningId = 17,
    )

  private val nextLedgerEnd = initialLedgerEnd.copy(lastOffset =
    Offset.fromHexString(Ref.HexString.assertFromString("abcdfe"))
  )

  private val thirdLedgerEnd = initialLedgerEnd.copy(lastOffset =
    Offset.fromHexString(Ref.HexString.assertFromString("abcdff"))
  )

  s"$className.{startDispatcher, stopDispatcher}" should "handle correctly the Dispatcher lifecycle" in {
    for {
      _ <- Future.unit
      dispatcherState = new DispatcherState(Duration.Zero)(loggingContext)
      // Start the initial Dispatcher
      _ = dispatcherState.startDispatcher(initialLedgerEnd)

      initialDispatcher = dispatcherState.getDispatcher

      // Stop the initial Dispatcher
      _ <- dispatcherState.stopDispatcher()
      // Assert that the initial Dispatcher reference does not accept new subscriptions
      _ <- assertDispatcherDoesntAcceptNewSubscriptions(initialDispatcher)

      // Getting the Dispatcher while stopped throws
      _ = intercept[IllegalStateException] {
        dispatcherState.getDispatcher
      }.getMessage shouldBe "Ledger API offset dispatcher not running."

      // Start a new Dispatcher
      _ = dispatcherState.startDispatcher(nextLedgerEnd)

      // Try to start a new Dispatcher
      _ = intercept[IllegalStateException] {
        dispatcherState.startDispatcher(thirdLedgerEnd)
      }.getMessage shouldBe "Dispatcher startup triggered while an existing dispatcher is still active."

      anotherDispatcher = dispatcherState.getDispatcher
    } yield {
      // Assert that the dispatcher instances are different
      initialDispatcher should not be anotherDispatcher
    }
  }

  s"$className.shutdown" should s"shutdown the $DispatcherState" in {
    for {
      _ <- Future.unit
      dispatcherState = new DispatcherState(Duration.Zero)(loggingContext)
      // Start the initial Dispatcher
      _ = dispatcherState.startDispatcher(initialLedgerEnd)

      initialDispatcher = dispatcherState.getDispatcher

      // Shutdown the state
      _ <- dispatcherState.shutdown()

      // Assert that the initial Dispatcher reference does not accept new subscriptions
      _ <- assertDispatcherDoesntAcceptNewSubscriptions(initialDispatcher)

      // Getting the Dispatcher while shutdown
      _ = intercept[IllegalStateException] {
        dispatcherState.getDispatcher
      }.getMessage shouldBe "Ledger API offset dispatcher state has already shut down."

      // Start a new Dispatcher is not possible in the shutdown state
      _ = intercept[IllegalStateException] {
        dispatcherState.startDispatcher(nextLedgerEnd)
      }.getMessage shouldBe "Ledger API offset dispatcher state has already shut down."
    } yield succeed
  }

  s"$className.shutdown" should s"work on not-running Dispatcher state" in {
    for {
      _ <- Future.unit
      // Start a new dispatcher state
      dispatcherState = new DispatcherState(Duration.Zero)(loggingContext)
      // Shutting down the state
      _ <- dispatcherState.shutdown()
      // Assert shutdown
      _ = intercept[IllegalStateException] {
        dispatcherState.getDispatcher
      }.getMessage shouldBe "Ledger API offset dispatcher state has already shut down."
      // Stopping the Dispatcher should be a no-op on a shutdown dispatcher
      _ <- dispatcherState.stopDispatcher()
    } yield succeed
  }

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
        case Failure(f) if f.getMessage == "ledger-api: Dispatcher is closed" =>
          Success(succeed)
        case other => fail(s"Unexpected result: $other")
      }
}
