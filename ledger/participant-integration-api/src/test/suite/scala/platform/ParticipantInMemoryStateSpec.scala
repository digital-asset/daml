// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.{Dispatcher, SubSource}
import com.daml.platform.store.backend.{ParameterStorageBackend, StringInterningStorageBackend}
import com.daml.platform.store.cache.{ContractStateCaches, EventsBuffer, MutableLedgerEndCache}
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.{LoadStringInterningEntries, StringInterningView}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class ParticipantInMemoryStateSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with AkkaBeforeAndAfterAll {
  private val className = classOf[ParticipantInMemoryState].getSimpleName
  private val metrics = new Metrics(new MetricRegistry)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  s"$className.initialized" should "return false if not initialized" in withTestFixture {
    case (participantInMemoryState, _, _, _, _) =>
      participantInMemoryState.initialized shouldBe false

      intercept[IllegalStateException] {
        participantInMemoryState.dispatcher()
      }.getMessage shouldBe "Uninitialized Ledger API offset dispatcher"
  }

  s"$className.initializedTo" should "initialize the state" in withTestFixture {
    case (
          participantInMemoryState,
          mutableLedgerEndCache,
          contractStateCaches,
          transactionsBuffer,
          stringInterningView,
        ) =>
      val initOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
      val initEventSequentialId = 1337L
      val initStringInterningId = 17

      val dbDispatcher = mock[DbDispatcher]
      val stringInterningStorageBackend = mock[StringInterningStorageBackend]

      for {
        // Initialize the state for the first time
        _ <- participantInMemoryState
          .initializedTo(
            ledgerEnd = ParameterStorageBackend
              .LedgerEnd(initOffset, initEventSequentialId, initStringInterningId)
          )(
            dbDispatcher = dbDispatcher,
            stringInterningStorageBackend = stringInterningStorageBackend,
          )

        // Assert that the state is initialized
        initialDispatcher = participantInMemoryState.dispatcher()
        _ = {
          participantInMemoryState._ledgerApiDispatcher should not be null
          initialDispatcher shouldBe a[Dispatcher[_]]

          verify(contractStateCaches).reset(initOffset)
          verify(transactionsBuffer).flush()

          verify(mutableLedgerEndCache).set(initOffset, initEventSequentialId)
          verify(stringInterningView).update(refEq(initStringInterningId))(
            any[LoadStringInterningEntries]
          )(any[LoggingContext])
        }

        // Reset mocks
        _ = {
          reset(mutableLedgerEndCache, contractStateCaches, transactionsBuffer, stringInterningView)
          when(
            stringInterningView.update(anyInt)(any[LoadStringInterningEntries])(any[LoggingContext])
          )
            .thenReturn(Future.unit)
        }

        // Re-initialize the state
        reInitOffset = Offset.fromHexString(Ref.HexString.assertFromString("abeeee"))
        reInitEventSequentialId = 9999L
        reInitStringInterningId = 50
        _ <- participantInMemoryState
          .initializedTo(
            ledgerEnd = ParameterStorageBackend
              .LedgerEnd(reInitOffset, reInitEventSequentialId, reInitStringInterningId)
          )(
            dbDispatcher = dbDispatcher,
            stringInterningStorageBackend = stringInterningStorageBackend,
          )

        // Assert that the state is re-initialized
        _ = {
          participantInMemoryState._ledgerApiDispatcher should not be null
          participantInMemoryState.dispatcher() should not be initialDispatcher

          verify(contractStateCaches).reset(reInitOffset)
          verify(transactionsBuffer).flush()

          verify(mutableLedgerEndCache).set(reInitOffset, reInitEventSequentialId)
          verify(stringInterningView).update(refEq(reInitStringInterningId))(
            any[LoadStringInterningEntries]
          )(any[LoggingContext])
        }
      } yield succeed
  }

  s"$className.shutdown" should "shutdown the state" in withTestFixture {
    case (
          participantInMemoryState,
          mutableLedgerEndCache,
          contractStateCaches,
          transactionsBuffer,
          stringInterningView,
        ) =>
      val initOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
      val initEventSequentialId = 1337L
      val initStringInterningId = 17

      for {
        _ <- participantInMemoryState
          .initializedTo(
            ledgerEnd = ParameterStorageBackend
              .LedgerEnd(initOffset, initEventSequentialId, initStringInterningId)
          )(
            dbDispatcher = mock[DbDispatcher],
            stringInterningStorageBackend = mock[StringInterningStorageBackend],
          )

        initialDispatcher = participantInMemoryState.dispatcher()
        // Assert state initialized
        _ = {
          participantInMemoryState._ledgerApiDispatcher should not be null
          initialDispatcher shouldBe a[Dispatcher[_]]

          verify(contractStateCaches).reset(initOffset)
          verify(transactionsBuffer).flush()

          verify(mutableLedgerEndCache).set(initOffset, initEventSequentialId)
          verify(stringInterningView).update(refEq(initStringInterningId))(
            any[LoadStringInterningEntries]
          )(any[LoggingContext])
        }
        // Shutdown the state
        _ <- participantInMemoryState.shutdown(Duration.fromNanos(1000L))
        _ <- assertDispatcherDoesntAcceptNewSubscriptions(initialDispatcher)
        _ = participantInMemoryState._ledgerApiDispatcher shouldBe null
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

  private def withTestFixture(
      test: (
          ParticipantInMemoryState,
          MutableLedgerEndCache,
          ContractStateCaches,
          EventsBuffer[TransactionLogUpdate],
          StringInterningView,
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val mutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches = mock[ContractStateCaches]

    val transactionsBuffer = mock[EventsBuffer[TransactionLogUpdate]]
    val stringInterningView = mock[StringInterningView]

    when(stringInterningView.update(anyInt)(any[LoadStringInterningEntries])(any[LoggingContext]))
      .thenReturn(Future.unit)

    val apiStreamShutdownDuration = Duration.fromNanos(1337L)

    val participantInMemoryState = new ParticipantInMemoryState(
      ledgerEndCache = mutableLedgerEndCache,
      contractStateCaches = contractStateCaches,
      transactionsBuffer = transactionsBuffer,
      stringInterningView = stringInterningView,
      apiStreamShutdownTimeout = apiStreamShutdownDuration,
      metrics = metrics,
    )

    test(
      participantInMemoryState,
      mutableLedgerEndCache,
      contractStateCaches,
      transactionsBuffer,
      stringInterningView,
    )
  }
}
