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

  s"$className.initialized" should "be initially false" in {
    val dispatcherState = new DispatcherState(Duration.Zero)(loggingContext)
    dispatcherState.initialized shouldBe false

    intercept[IllegalStateException] {
      dispatcherState.getDispatcher
    }.getMessage shouldBe "Uninitialized Ledger API offset dispatcher."
  }

  s"$className.reset" should "reset the dispatcher" in {
    val dispatcherState = new DispatcherState(Duration.Zero)(loggingContext)

    for {
      _ <- dispatcherState.reset(
        ParameterStorageBackend
          .LedgerEnd(
            lastOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef")),
            lastEventSeqId = 1337L,
            lastStringInterningId = 17,
          )
      )

      initialDispatcher = {
        dispatcherState.initialized shouldBe true
        dispatcherState.getDispatcher
      }

      _ <- dispatcherState.reset(
        ParameterStorageBackend
          .LedgerEnd(
            lastOffset = Offset.fromHexString(Ref.HexString.assertFromString("abceee")),
            lastEventSeqId = 1338L,
            lastStringInterningId = 18,
          )
      )

      anotherDispatcher = dispatcherState.getDispatcher
    } yield {
      // Assert that the dispatcher instances are different
      initialDispatcher should not be anotherDispatcher
    }
  }

  s"$className.shutdown" should "shutdown the dispatcher" in {
    val dispatcherState = new DispatcherState(Duration.Zero)(loggingContext)

    for {
      _ <- dispatcherState.reset(
        ParameterStorageBackend
          .LedgerEnd(
            lastOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef")),
            lastEventSeqId = 1337L,
            lastStringInterningId = 17,
          )
      )

      initialDispatcher = {
        dispatcherState.initialized shouldBe true
        dispatcherState.getDispatcher
      }

      _ <- dispatcherState.shutdown()

      // Assert dispatcher state shutdown
      _ = {
        dispatcherState.initialized shouldBe false
        intercept[IllegalStateException] {
          dispatcherState.getDispatcher
        }.getMessage shouldBe "Ledger API offset dispatcher has already shut down."
      }

      // Assert old dispatcher reference shutdown
      _ <- assertDispatcherDoesntAcceptNewSubscriptions(initialDispatcher)
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
