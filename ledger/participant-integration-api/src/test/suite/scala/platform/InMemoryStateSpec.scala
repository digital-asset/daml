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
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.{StringInterningView, UpdatingStringInterningView}
import org.mockito.{ArgumentMatchersSugar, InOrder, Mockito, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.util.{Failure, Success}

class InMemoryStateSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {
  private val className = classOf[InMemoryState].getSimpleName
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val initOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
  private val initEventSequentialId = 1337L
  private val initStringInterningId = 17
  private val initLedgerEnd = ParameterStorageBackend
    .LedgerEnd(initOffset, initEventSequentialId, initStringInterningId)

  s"$className.initialized" should "return true if dispatcherState is running" in withTestFixture {
    case (inMemoryState, _, _) =>
      when(inMemoryState.dispatcherState.isRunning).thenReturn(false)
      inMemoryState.initialized shouldBe false

      when(inMemoryState.dispatcherState.isRunning).thenReturn(true)
      inMemoryState.initialized shouldBe true
  }

  s"$className.initializeTo" should "initialize the state" in withTestFixture {
    case (
          inMemoryState,
          updateStringInterningView,
          inOrder,
        ) =>
      for {
        // Initialize the state
        _ <- inMemoryState.initializeTo(initLedgerEnd, executionContext)(updateStringInterningView)

        _ = {
          // Assert the state initialized
          verifyInitializedInOrder(inMemoryState, updateStringInterningView, inOrder, initLedgerEnd)
        }
      } yield succeed
  }

  s"$className.initializeTo" should "not re-initialize if the conditions did not change" in withTestFixture {
    case (
          inMemoryState,
          updateStringInterningView,
          _,
        ) =>
      import inMemoryState._

      arrangeInitialized(inMemoryState, initLedgerEnd)

      for {
        // Re-initialize to the same ledger end
        _ <- inMemoryState.initializeTo(initLedgerEnd, executionContext)(updateStringInterningView)
        _ = {
          // Ledger end references checks
          verify(dispatcherState).isRunning
          verify(ledgerEndCache).apply()
          verify(stringInterningView).lastId

          // Assert no effect
          verifyNoMoreInteractions(
            dispatcherState,
            updateStringInterningView,
            contractStateCaches,
            inMemoryFanoutBuffer,
            ledgerEndCache,
            stringInterningView,
          )
        }
      } yield succeed
  }

  s"$className.initializeTo" should "re-initialize the state on changed ledger end cache" in withTestFixture {
    case (
          inMemoryState,
          updateStringInterningView,
          inOrder,
        ) =>
      import inMemoryState._

      val reInitLedgerEnd = initLedgerEnd.copy(lastOffset =
        Offset.fromHexString(Ref.HexString.assertFromString("abeeee"))
      )
      arrangeInitialized(inMemoryState, initLedgerEnd)
      when(updateStringInterningView(stringInterningView, reInitLedgerEnd)).thenReturn(
        Future.unit
      )

      for {
        // Re-initialize to a different ledger end
        _ <- inMemoryState.initializeTo(reInitLedgerEnd, executionContext)(
          updateStringInterningView
        )

        _ = {
          // Ledger end references checks
          verify(dispatcherState).isRunning
          verify(ledgerEndCache).apply()
          // Short-circuited by the failing ledger end cache check
          verify(stringInterningView, never).lastId

          // Assert state initialized to the new ledger end
          verifyInitializedInOrder(
            inMemoryState,
            updateStringInterningView,
            inOrder,
            reInitLedgerEnd,
          )
        }
      } yield succeed
  }

  s"$className.initializeTo" should "re-initialize the state on changed last string interned id" in withTestFixture {
    case (
          inMemoryState,
          updateStringInterningView,
          inOrder,
        ) =>
      import inMemoryState._

      val newLedgerEnd =
        initLedgerEnd.copy(lastStringInterningId = initLedgerEnd.lastStringInterningId + 1)

      arrangeInitialized(inMemoryState, initLedgerEnd)
      when(updateStringInterningView(stringInterningView, newLedgerEnd)).thenReturn(
        Future.unit
      )

      for {
        // Re-initialize to a different string interning id
        _ <- inMemoryState.initializeTo(newLedgerEnd, executionContext)(
          updateStringInterningView
        )

        _ = {
          // Ledger end references checks
          verify(dispatcherState).isRunning
          verify(ledgerEndCache).apply()
          verify(stringInterningView).lastId

          // Assert state initialized to the new ledger end
          verifyInitializedInOrder(
            inMemoryState,
            updateStringInterningView,
            inOrder,
            newLedgerEnd,
          )
        }
      } yield succeed
  }

  s"$className.initializeTo" should "re-initialize the state on dirty update" in withTestFixture {
    case (
          inMemoryState,
          updateStringInterningView,
          inOrder,
        ) =>
      import inMemoryState._

      arrangeInitialized(inMemoryState, initLedgerEnd)

      val failingInitOffset = Offset.fromHexString(Ref.HexString.assertFromString("abeeee"))
      val failingInitEventSeqId = 9999L

      for {
        // Set dirty by simulating a failure and an external recovery
        // while attempting to set the ledger end to reInitOffset and reInitEventSequentialId
        _ <- failedUpdate(inMemoryState, failingInitOffset, failingInitEventSeqId)

        // Trying to update the state on dirty state fails
        _ <- recoverToSucceededIf[IllegalStateException] {
          inMemoryState.update(
            Vector(mock[TransactionLogUpdate] -> Vector.empty),
            failingInitOffset,
            failingInitEventSeqId,
          )
        }

        // Re-initialize the state to the same ledger end
        _ <- inMemoryState.initializeTo(initLedgerEnd, executionContext)(updateStringInterningView)

        _ = {
          // Ledger end references checks short-circuited by dirty check
          verify(stringInterningView, never).lastId
          verify(ledgerEndCache, never).apply()

          // Assert state re-initialized on dirty
          verifyInitializedInOrder(inMemoryState, updateStringInterningView, inOrder, initLedgerEnd)
        }
      } yield succeed
  }

  s"$className.initializeTo" should "re-initialize the state on dirty initializeTo" in withTestFixture {
    case (
          inMemoryState,
          updateStringInterningView,
          inOrder,
        ) =>
      import inMemoryState._

      arrangeInitialized(inMemoryState, initLedgerEnd)

      val failingLedgerEnd = initLedgerEnd.copy(lastOffset =
        Offset.fromHexString(Ref.HexString.assertFromString("abeeee"))
      )

      for {
        // Set dirty by simulating a failure and an external recovery
        _ <- failingInitializeTo(inMemoryState, failingLedgerEnd)

        _ = {
          reset(dispatcherState, ledgerEndCache)
          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
          when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(
            Future.unit
          )
          when(dispatcherState.isRunning).thenReturn(true)
        }
        // Re-initialize the state to the same ledger end
        _ <- inMemoryState.initializeTo(initLedgerEnd, executionContext)(updateStringInterningView)

        _ = {
          verify(dispatcherState).isRunning
          // Ledger end reference checks short-circuited by dirty check
          verify(ledgerEndCache, never).apply()
          verify(stringInterningView, never).lastId

          // Assert state re-initialized on dirty
          verifyInitializedInOrder(inMemoryState, updateStringInterningView, inOrder, initLedgerEnd)
        }
      } yield succeed
  }

  private def verifyInitializedInOrder(
      inMemoryState: InMemoryState,
      updateStringInterningView: (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
      inOrder: InOrder,
      initLedgerEnd: LedgerEnd,
  ): Unit = {
    import inMemoryState._

    inOrder.verify(dispatcherState).stopDispatcher()
    inOrder.verify(updateStringInterningView)(stringInterningView, initLedgerEnd)
    inOrder.verify(contractStateCaches).reset(initLedgerEnd.lastOffset)
    inOrder.verify(inMemoryFanoutBuffer).flush()
    inOrder.verify(ledgerEndCache).set(initLedgerEnd.lastOffset, initLedgerEnd.lastEventSeqId)
    inOrder.verify(dispatcherState).startDispatcher(initLedgerEnd.lastOffset)
  }

  private def failedUpdate(
      inMemoryState: InMemoryState,
      offset: Offset,
      eventSeqId: Long,
  ): Future[Unit] = {
    val failureMessage = "failed"
    when(inMemoryState.inMemoryFanoutBuffer.push(any[Offset], any[TransactionLogUpdate]))
      .thenThrow(new RuntimeException(failureMessage))

    inMemoryState
      .update(
        updates = Vector(mock[TransactionLogUpdate] -> Vector.empty),
        lastOffset = offset,
        lastEventSequentialId = eventSeqId,
      )
      .transform {
        case Failure(ex: RuntimeException) if ex.getMessage == failureMessage => Success(())
        case other => fail(s"Unexpected $other")
      }
  }

  private def failingInitializeTo(
      inMemoryState: InMemoryState,
      ledgerEnd: LedgerEnd,
  ): Future[Unit] = {
    val failureMessage = "failed"
    when(inMemoryState.dispatcherState.stopDispatcher())
      .thenThrow(new RuntimeException(failureMessage))

    inMemoryState
      .initializeTo(ledgerEnd = ledgerEnd, executionContext = executionContext)(
        updateStringInterningView = null // Not used
      )
      .transform {
        case Failure(ex: RuntimeException) if ex.getMessage == failureMessage => Success(())
        case other => fail(s"Unexpected $other")
      }
  }

  private def withTestFixture(
      test: (
          InMemoryState,
          (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
          InOrder,
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val ledgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches = mock[ContractStateCaches]
    val inMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView = mock[StringInterningView]
    val dispatcherState = mock[DispatcherState]
    val updateStringInterningView = mock[(UpdatingStringInterningView, LedgerEnd) => Future[Unit]]

    // Mocks should be called in the asserted order
    val inOrderMockCalls = Mockito.inOrder(
      ledgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
    )

    // Arrange non-initialized state
    when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(Future.unit)
    when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
    when(dispatcherState.isRunning).thenReturn(false)

    val inMemoryState = new InMemoryState(
      ledgerEndCache = ledgerEndCache,
      contractStateCaches = contractStateCaches,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
    )

    test(
      inMemoryState,
      updateStringInterningView,
      inOrderMockCalls,
    )
  }

  private def arrangeInitialized(
      inMemoryState: InMemoryState,
      initializedTo: LedgerEnd,
  ): Unit = {
    import inMemoryState._

    when(ledgerEndCache()).thenReturn(initializedTo.lastOffset -> initializedTo.lastEventSeqId)
    when(stringInterningView.lastId).thenReturn(initializedTo.lastStringInterningId)
    when(dispatcherState.isRunning).thenReturn(true)
    ()
  }
}
