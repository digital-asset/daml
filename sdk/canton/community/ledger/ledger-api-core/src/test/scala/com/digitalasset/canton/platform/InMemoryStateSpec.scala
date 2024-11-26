// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp}
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
  OffsetCheckpointCache,
}
import com.digitalasset.canton.platform.store.interning.{
  StringInterningView,
  UpdatingStringInterningView,
}
import org.mockito.{InOrder, Mockito, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class InMemoryStateSpec extends AsyncFlatSpec with MockitoSugar with Matchers with TestEssentials {
  private val className = classOf[InMemoryState].getSimpleName

  s"$className.initialized" should "return false if not initialized" in withTestFixture {
    case (inMemoryState, _, _, _, _, _, _, _, _) =>
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
          submissionTracker,
          inOrder,
        ) =>
      val initOffset = AbsoluteOffset.tryFromLong(12345678L)
      val initEventSequentialId = 1337L
      val initStringInterningId = 17
      val initPublicationTime = CantonTimestamp.now()

      val initLedgerEnd = ParameterStorageBackend
        .LedgerEnd(initOffset, initEventSequentialId, initStringInterningId, initPublicationTime)

      when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(Future.unit)
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
      when(dispatcherState.isRunning).thenReturn(true)
      when(mutableLedgerEndCache.apply()).thenReturn(None)
      when(dispatcherState.getDispatcher).thenReturn(
        Dispatcher(
          name = "",
          firstIndex = AbsoluteOffset.firstOffset,
          headAtInitialization = None,
        )
      )

      for {
        // INITIALIZED THE STATE
        _ <- inMemoryState.initializeTo(Some(initLedgerEnd))

        _ = {
          // ASSERT STATE INITIALIZED

          inOrder.verify(dispatcherState).stopDispatcher()
          inOrder.verify(contractStateCaches).reset(Some(initOffset))
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder
            .verify(mutableLedgerEndCache)
            .set(Some(initLedgerEnd))
          inOrder.verify(submissionTracker).close()
          inOrder
            .verify(dispatcherState)
            .startDispatcher(Some(initLedgerEnd.lastOffset))

          inMemoryState.initialized shouldBe true
        }

        reInitOffset = AbsoluteOffset.tryFromLong(12345678L)
        reInitEventSequentialId = 9999L
        reInitStringInterningId = 50
        reInitPublicationTime = CantonTimestamp.now()
        reInitLedgerEnd = ParameterStorageBackend
          .LedgerEnd(
            reInitOffset,
            reInitEventSequentialId,
            reInitStringInterningId,
            reInitPublicationTime,
          )

        // RESET MOCKS
        _ = {
          reset(
            mutableLedgerEndCache,
            contractStateCaches,
            inMemoryFanoutBuffer,
            updateStringInterningView,
          )
          when(updateStringInterningView(stringInterningView, reInitLedgerEnd)).thenReturn(
            Future.unit
          )

          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
          when(mutableLedgerEndCache.apply()).thenReturn(Some(initLedgerEnd))
          when(dispatcherState.getDispatcher).thenReturn(
            Dispatcher(
              name = "",
              firstIndex = AbsoluteOffset.firstOffset,
              headAtInitialization = Some(initOffset),
            )
          )
        }

        // RE-INITIALIZE THE STATE
        _ <- inMemoryState.initializeTo(Some(reInitLedgerEnd))

        // ASSERT STATE RE-INITIALIZED
        _ = {
          inOrder.verify(dispatcherState).stopDispatcher()

          when(dispatcherState.isRunning).thenReturn(false)
          inMemoryState.initialized shouldBe false
          inOrder.verify(contractStateCaches).reset(Some(reInitOffset))
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder
            .verify(mutableLedgerEndCache)
            .set(Some(reInitLedgerEnd))
          inOrder.verify(dispatcherState).startDispatcher(Some(reInitOffset))

          when(dispatcherState.isRunning).thenReturn(true)
          inMemoryState.initialized shouldBe true
        }

        // RE-INITIALIZE THE SAME STATE
        _ <- inMemoryState.initializeTo(Some(reInitLedgerEnd))

        // ASSERT STATE RE-INITIALIZED
        _ = inMemoryState.initialized shouldBe true
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
          SubmissionTracker,
          InOrder,
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val mutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches = mock[ContractStateCaches]
    val offsetCheckpointCache = mock[OffsetCheckpointCache]
    val inMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView = mock[StringInterningView]
    val dispatcherState = mock[DispatcherState]
    val updateStringInterningView = mock[(UpdatingStringInterningView, LedgerEnd) => Future[Unit]]
    val submissionTracker = mock[SubmissionTracker]
    val commandProgressTracker = CommandProgressTracker.NoOp

    // Mocks should be called in the asserted order
    val inOrderMockCalls = Mockito.inOrder(
      mutableLedgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
      submissionTracker,
    )

    val inMemoryState = new InMemoryState(
      ledgerEndCache = mutableLedgerEndCache,
      contractStateCaches = contractStateCaches,
      offsetCheckpointCache = offsetCheckpointCache,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
      submissionTracker = submissionTracker,
      commandProgressTracker = commandProgressTracker,
      loggerFactory = loggerFactory,
    )

    test(
      inMemoryState,
      mutableLedgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
      submissionTracker,
      inOrderMockCalls,
    )
  }
}
