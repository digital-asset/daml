// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.lf.data.Ref
import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
}
import com.digitalasset.canton.platform.store.interning.{
  StringInterningView,
  UpdatingStringInterningView,
}
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataView
import org.mockito.{InOrder, Mockito, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class InMemoryStateSpec extends AsyncFlatSpec with MockitoSugar with Matchers with TestEssentials {
  private val className = classOf[InMemoryState].getSimpleName

  s"$className.initialized" should "return false if not initialized" in withTestFixture {
    case (inMemoryState, _, _, _, _, _, _, _, _, _, _) =>
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
          packageMetadataView,
          updateStringInterningView,
          updatePackageMetadataView,
          submissionTracker,
          inOrder,
        ) =>
      val initOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
      val initEventSequentialId = 1337L
      val initStringInterningId = 17

      val initLedgerEnd = ParameterStorageBackend
        .LedgerEnd(initOffset, initEventSequentialId, initStringInterningId)

      when(updateStringInterningView(stringInterningView, initLedgerEnd)).thenReturn(Future.unit)
      when(updatePackageMetadataView(packageMetadataView)).thenReturn(Future.unit)
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
      when(dispatcherState.isRunning).thenReturn(true)

      for {
        // INITIALIZED THE STATE
        _ <- inMemoryState.initializeTo(initLedgerEnd)(
          updateStringInterningView,
          updatePackageMetadataView,
        )

        _ = {
          // ASSERT STATE INITIALIZED

          inOrder.verify(dispatcherState).stopDispatcher()
          inOrder.verify(updateStringInterningView)(stringInterningView, initLedgerEnd)
          inOrder.verify(updatePackageMetadataView)(packageMetadataView)
          inOrder.verify(contractStateCaches).reset(initOffset)
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder.verify(mutableLedgerEndCache).set(initOffset -> initEventSequentialId)
          inOrder.verify(submissionTracker).close()
          inOrder.verify(dispatcherState).startDispatcher(initLedgerEnd.lastOffset)

          inMemoryState.initialized shouldBe true
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
            inMemoryFanoutBuffer,
            updateStringInterningView,
            updatePackageMetadataView,
          )
          when(updateStringInterningView(stringInterningView, reInitLedgerEnd)).thenReturn(
            Future.unit
          )
          when(updatePackageMetadataView(packageMetadataView)).thenReturn(Future.unit)

          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
        }

        // RE-INITIALIZE THE STATE
        _ <- inMemoryState.initializeTo(reInitLedgerEnd)(
          {
            case (`stringInterningView`, ledgerEnd: LedgerEnd) =>
              updateStringInterningView(stringInterningView, ledgerEnd)
            case (other, _) => fail(s"Unexpected stringInterningView reference $other")
          },
          updatePackageMetadataView,
        )

        // ASSERT STATE RE-INITIALIZED
        _ = {
          inOrder.verify(dispatcherState).stopDispatcher()

          when(dispatcherState.isRunning).thenReturn(false)
          inMemoryState.initialized shouldBe false
          inOrder.verify(updateStringInterningView)(stringInterningView, reInitLedgerEnd)
          inOrder.verify(updatePackageMetadataView)(packageMetadataView)
          inOrder.verify(contractStateCaches).reset(reInitOffset)
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder.verify(mutableLedgerEndCache).set(reInitOffset -> reInitEventSequentialId)
          inOrder.verify(dispatcherState).startDispatcher(reInitOffset)

          when(dispatcherState.isRunning).thenReturn(true)
          inMemoryState.initialized shouldBe true
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
          PackageMetadataView,
          (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
          PackageMetadataView => Future[Unit],
          SubmissionTracker,
          InOrder,
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val mutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches = mock[ContractStateCaches]
    val inMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView = mock[StringInterningView]
    val dispatcherState = mock[DispatcherState]
    val packageMetadataView = mock[PackageMetadataView]
    val updateStringInterningView = mock[(UpdatingStringInterningView, LedgerEnd) => Future[Unit]]
    val updatePackageMetadataView = mock[PackageMetadataView => Future[Unit]]
    val submissionTracker = mock[SubmissionTracker]

    // Mocks should be called in the asserted order
    val inOrderMockCalls = Mockito.inOrder(
      mutableLedgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
      updatePackageMetadataView,
      submissionTracker,
    )

    val inMemoryState = new InMemoryState(
      ledgerEndCache = mutableLedgerEndCache,
      contractStateCaches = contractStateCaches,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
      packageMetadataView = packageMetadataView,
      submissionTracker = submissionTracker,
      loggerFactory = loggerFactory,
    )

    test(
      inMemoryState,
      mutableLedgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      packageMetadataView,
      updateStringInterningView,
      updatePackageMetadataView,
      submissionTracker,
      inOrderMockCalls,
    )
  }
}
