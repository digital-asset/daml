// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.ParameterStorageBackend
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{ContractStateCaches, EventsBuffer, MutableLedgerEndCache}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.StringInterningView
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class InMemoryStateSpec extends AsyncFlatSpec with MockitoSugar with Matchers {
  private val className = classOf[InMemoryState].getSimpleName
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  s"$className.initialized" should "return false if not initialized" in withTestFixture {
    case (inMemoryState, _, _, _, _, _) =>
      inMemoryState.initialized shouldBe false
  }

  s"$className.initializeTo" should "initialize the state" in withTestFixture {
    case (
          inMemoryState,
          mutableLedgerEndCache,
          contractStateCaches,
          transactionsBuffer,
          updateStringInterningView,
          dispatcherState,
        ) =>
      val initOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
      val initEventSequentialId = 1337L
      val initStringInterningId = 17

      val initLedgerEnd = ParameterStorageBackend
        .LedgerEnd(initOffset, initEventSequentialId, initStringInterningId)

      when(updateStringInterningView(initLedgerEnd)).thenReturn(Future.unit)
      when(dispatcherState.reset(initLedgerEnd)).thenReturn(Future.unit)
      when(dispatcherState.initialized).thenReturn(true)
      for {
        // INITIALIZED THE STATE
        _ <- inMemoryState.initializeTo(initLedgerEnd)(updateStringInterningView)

        // ASSERT STATE INITIALIZED
        _ = {
          inMemoryState.initialized shouldBe true

          verify(contractStateCaches).reset(initOffset)
          verify(transactionsBuffer).flush()
          verify(mutableLedgerEndCache).set(initOffset, initEventSequentialId)
          verify(updateStringInterningView)(initLedgerEnd)
          verify(dispatcherState).reset(initLedgerEnd)
        }

        reInitOffset = Offset.fromHexString(Ref.HexString.assertFromString("abeeee"))
        reInitEventSequentialId = 9999L
        reInitStringInterningId = 50
        reInitLedgerEnd = ParameterStorageBackend
          .LedgerEnd(reInitOffset, reInitEventSequentialId, reInitStringInterningId)

        // RESET MOCKS
        _ = {
          reset(
            mutableLedgerEndCache,
            contractStateCaches,
            transactionsBuffer,
            updateStringInterningView,
          )
          when(updateStringInterningView(reInitLedgerEnd)).thenReturn(
            Future.unit
          )
          when(dispatcherState.reset(reInitLedgerEnd)).thenReturn(Future.unit)
        }

        // RE-INITIALIZE THE STATE
        _ <- inMemoryState.initializeTo(reInitLedgerEnd) {
          case (`stringInterningView`, ledgerEnd) =>
            updateStringInterningView(stringInterningView, ledgerEnd)
          case (other, _) => fail(s"Unexpected stringInterningView reference $other")
        }

        // ASSERT STATE RE-INITIALIZED
        _ = {
          inMemoryState.initialized shouldBe true

          verify(contractStateCaches).reset(reInitOffset)
          verify(transactionsBuffer).flush()
          verify(mutableLedgerEndCache).set(reInitOffset, reInitEventSequentialId)
          verify(updateStringInterningView)(reInitLedgerEnd)
          verify(dispatcherState).reset(reInitLedgerEnd)
        }
      } yield succeed
  }

  private def withTestFixture(
      test: (
          InMemoryState,
          MutableLedgerEndCache,
          ContractStateCaches,
          EventsBuffer[TransactionLogUpdate],
          LedgerEnd => Future[Unit],
          DispatcherState,
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val mutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches = mock[ContractStateCaches]

    val transactionsBuffer = mock[EventsBuffer[TransactionLogUpdate]]
    val stringInterningView = mock[StringInterningView]

    val dispatcherState = mock[DispatcherState]

    val inMemoryState = new InMemoryState(
      ledgerEndCache = mutableLedgerEndCache,
      contractStateCaches = contractStateCaches,
      transactionsBuffer = transactionsBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
      updateStringInterningView = {
        case (`stringInterningView`, ledgerEnd) => updateStringInterningViewWithLedgerEnd(ledgerEnd)
        case (other, _) => fail(s"Unexpected stringInterningView reference $other")
      },
    )

    test(
      inMemoryState,
      mutableLedgerEndCache,
      contractStateCaches,
      transactionsBuffer,
      updateStringInterningViewWithLedgerEnd,
      dispatcherState,
    )
  }
}
