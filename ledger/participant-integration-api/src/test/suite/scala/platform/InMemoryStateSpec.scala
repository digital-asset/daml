// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.ParameterStorageBackend
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interning.{StringInterningView, UpdatingStringInterningView}
import org.mockito.{InOrder, Mockito, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class InMemoryStateSpec extends AsyncFlatSpec with MockitoSugar with Matchers {
  private val className = classOf[InMemoryState].getSimpleName
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val initOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
  private val initEventSequentialId = 1337L
  private val initStringInterningId = 17
  private val initLedgerEnd = ParameterStorageBackend
    .LedgerEnd(initOffset, initEventSequentialId, initStringInterningId)

  private val reInitOffset = Offset.fromHexString(Ref.HexString.assertFromString("abeeee"))
  private val reInitEventSequentialId = 9999L
  private val reInitStringInterningId = 50
  private val reInitLedgerEnd = ParameterStorageBackend
    .LedgerEnd(reInitOffset, reInitEventSequentialId, reInitStringInterningId)

  s"$className.initialized" should "return false if not initialized" in withTestFixture {
    case (inMemoryState, _, _, _, _, _, _, _) =>
      inMemoryState.initialized shouldBe false
  }

  s"$className.initializeTo" should "initialize the state" in withTestFixture {
    case (
          inMemoryState,
          mutableLedgerEndCache,
          contractStateCaches,
          inMemoryFanoutBuffer,
          stringInterningView,
          dispatcherState,
          updateStringInterningView,
          inOrder,
        ) =>
      when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(Future.unit)
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)

      for {
        // Initialize the state
        _ <- inMemoryState.initializeTo(initLedgerEnd)(updateStringInterningView)
        _ = {
          // Assert the state initialized
          inOrder.verify(dispatcherState).stopDispatcher()
          inOrder.verify(updateStringInterningView)(stringInterningView, initLedgerEnd)
          inOrder.verify(contractStateCaches).reset(initOffset)
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder.verify(mutableLedgerEndCache).set(initOffset, initEventSequentialId)
          inOrder.verify(dispatcherState).startDispatcher(initLedgerEnd)

          inMemoryState.initialized shouldBe true
        }

        _ = {
          when(mutableLedgerEndCache()).thenReturn(initOffset -> initEventSequentialId)
          when(stringInterningView.lastId).thenReturn(initStringInterningId)
          when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(
            Future.unit
          )
          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
        }

        // Re-initialize to the same ledger end
        _ <- inMemoryState.initializeTo(initLedgerEnd)(updateStringInterningView)
        _ = {
          // Ledger end references checks
          verify(mutableLedgerEndCache).apply()
          verify(stringInterningView).lastId

          // ASSERT NO EFFECT
          verifyNoMoreInteractions(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            mutableLedgerEndCache,
            stringInterningView,
          )

          inMemoryState.initialized shouldBe true
        }
      } yield succeed
  }

  s"$className.initializeTo" should "re-initialize the state on changed ledger end cache" in withTestFixture {
    case (
          inMemoryState,
          mutableLedgerEndCache,
          contractStateCaches,
          inMemoryFanoutBuffer,
          stringInterningView,
          dispatcherState,
          updateStringInterningView,
          inOrder,
        ) =>
      when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(Future.unit)
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)

      for {
        // Initialize the state to the initial ledger end
        _ <- inMemoryState.initializeTo(initLedgerEnd)(updateStringInterningView)

        // Reset mocks
        _ = {
          reset(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            mutableLedgerEndCache,
            stringInterningView,
          )
          when(mutableLedgerEndCache()).thenReturn(initOffset -> initEventSequentialId)
          when(stringInterningView.lastId).thenReturn(initStringInterningId)
          when(updateStringInterningView(stringInterningView, reInitLedgerEnd)).thenReturn(
            Future.unit
          )
          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
        }

        // Re-initialize to a different ledger end
        _ <- inMemoryState.initializeTo(reInitLedgerEnd)(updateStringInterningView)

        _ = {
          // Ledger end references checks
          verify(mutableLedgerEndCache).apply()
          // Short-circuited by the failing ledger end cache check
          verify(stringInterningView, never).lastId

          // Assert state initialized to the new ledger end
          inOrder.verify(dispatcherState).stopDispatcher()
          inOrder.verify(updateStringInterningView)(stringInterningView, reInitLedgerEnd)
          inOrder.verify(contractStateCaches).reset(reInitOffset)
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder.verify(mutableLedgerEndCache).set(reInitOffset, reInitEventSequentialId)
          inOrder.verify(dispatcherState).startDispatcher(reInitLedgerEnd)
        }

        // Reset mocks
        _ = {
          reset(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            mutableLedgerEndCache,
            stringInterningView,
          )
          when(mutableLedgerEndCache()).thenReturn(reInitOffset -> reInitEventSequentialId)
          when(stringInterningView.lastId).thenReturn(reInitStringInterningId)
        }

        // Attempt to re-initialize to the same ledger end
        _ <- inMemoryState.initializeTo(reInitLedgerEnd)(updateStringInterningView)
        _ = {
          // Assert ledger end reference checks
          verify(stringInterningView).lastId
          verify(mutableLedgerEndCache).apply()

          // Assert no effect
          verifyNoMoreInteractions(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            mutableLedgerEndCache,
            stringInterningView,
          )
        }
      } yield succeed
  }

  s"$className.initializeTo" should "initialize the on dirty" in withTestFixture {
    case (
          inMemoryState,
          mutableLedgerEndCache,
          contractStateCaches,
          inMemoryFanoutBuffer,
          stringInterningView,
          dispatcherState,
          updateStringInterningView,
          inOrder,
        ) =>
      when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(Future.unit)
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)

      for {
        // Initialize the state
        _ <- inMemoryState.initializeTo(initLedgerEnd)(updateStringInterningView)

        // reset mocks
        _ = {
          reset(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            mutableLedgerEndCache,
            stringInterningView,
          )
          when(mutableLedgerEndCache()).thenReturn(initOffset -> initEventSequentialId)
          when(stringInterningView.lastId).thenReturn(initStringInterningId)
          when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(
            Future.unit
          )
          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
        }

        // Set dirty
        _ = inMemoryState.setDirty()

        // Re-initialize the state to the same ledger end
        _ <- inMemoryState.initializeTo(initLedgerEnd)(updateStringInterningView)

        _ = {
          // Ledger end references checks short-circuited by dirty check
          verify(stringInterningView, never).lastId
          verify(mutableLedgerEndCache, never).apply()

          // Assert state re-initialized on dirty
          inOrder.verify(dispatcherState).stopDispatcher()
          inOrder.verify(updateStringInterningView)(stringInterningView, initLedgerEnd)
          inOrder.verify(contractStateCaches).reset(initOffset)
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder.verify(mutableLedgerEndCache).set(initOffset, initEventSequentialId)
          inOrder.verify(dispatcherState).startDispatcher(initLedgerEnd)
        }

        // reset mocks
        _ = {
          reset(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            mutableLedgerEndCache,
            stringInterningView,
          )
          when(mutableLedgerEndCache()).thenReturn(initOffset -> initEventSequentialId)
          when(stringInterningView.lastId).thenReturn(initStringInterningId)
          when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(
            Future.unit
          )
          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
        }

        // Re-initialize the state to the same ledger end
        _ <- inMemoryState.initializeTo(initLedgerEnd)(updateStringInterningView)
        _ = {
          // Ledger end references checks
          verify(mutableLedgerEndCache).apply()
          verify(stringInterningView).lastId

          // Assert no effect on state not dirty anymore
          verifyNoMoreInteractions(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            mutableLedgerEndCache,
          )
        }
      } yield succeed
  }

  private def withTestFixture(
      test: (
          InMemoryState,
          MutableLedgerEndCache,
          ContractStateCaches,
          InMemoryFanoutBuffer,
          StringInterningView,
          DispatcherState,
          (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
          InOrder,
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val mutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches = mock[ContractStateCaches]
    val inMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView = mock[StringInterningView]
    val dispatcherState = mock[DispatcherState]
    val updateStringInterningView = mock[(UpdatingStringInterningView, LedgerEnd) => Future[Unit]]

    // Mocks should be called in the asserted order
    val inOrderMockCalls = Mockito.inOrder(
      mutableLedgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
    )

    val inMemoryState = new InMemoryState(
      ledgerEndCache = mutableLedgerEndCache,
      contractStateCaches = contractStateCaches,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
    )

    test(
      inMemoryState,
      mutableLedgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
      inOrderMockCalls,
    )
  }
}
