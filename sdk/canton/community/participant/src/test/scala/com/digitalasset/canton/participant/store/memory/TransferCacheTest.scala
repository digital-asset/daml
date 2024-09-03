// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{Chain, EitherT}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.{IncompleteTransferData, TransferData}
import com.digitalasset.canton.participant.store.TransferStore.*
import com.digitalasset.canton.participant.store.memory.TransferCacheTest.HookTransferStore
import com.digitalasset.canton.participant.store.{TransferStore, TransferStoreTest}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{ReassignmentId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.{BaseTest, HasExecutorService, LfPartyId, RequestCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class TransferCacheTest extends AsyncWordSpec with BaseTest with HasExecutorService {
  import TransferStoreTest.*

  val transferDataF =
    mkTransferDataForDomain(
      transfer10,
      mediator1,
      targetDomainId = TransferStoreTest.targetDomain,
    )
  val toc = TimeOfChange(RequestCounter(0), CantonTimestamp.Epoch)

  "find transfers in the backing store" in {
    val store = new InMemoryTransferStore(targetDomain, loggerFactory)
    val cache = new TransferCache(store, loggerFactory)

    for {
      transferData <- transferDataF
      _ <- valueOrFail(store.addTransfer(transferData).failOnShutdown)("add failed")
      _ <- valueOrFail(store.lookup(transfer10))("lookup did not find transfer")
      lookup11 <- cache.lookup(transfer11).value
      () <- store.deleteTransfer(transfer10)
      deleted <- cache.lookup(transfer10).value
    } yield {
      lookup11 shouldBe Left(UnknownReassignmentId(transfer11))
      deleted shouldBe Left(UnknownReassignmentId(transfer10))
    }
  }

  "completeTransfer" should {
    "immediately report the transfer as completed" in {
      val backingStore = new InMemoryTransferStore(targetDomain, loggerFactory)
      val store = new HookTransferStore(backingStore)
      val cache = new TransferCache(store, loggerFactory)
      for {
        transferData <- transferDataF
        _ <- valueOrFail(store.addTransfer(transferData).failOnShutdown)("add failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == transfer10)
          CheckedT(
            cache.lookup(transfer10).value.map {
              case Left(TransferCompleted(`transfer10`, `toc`)) => Checked.result(())
              case result => fail(s"Invalid lookup result $result")
            }
          )
        }
        _ <- valueOrFail(cache.completeTransfer(transfer10, toc))("first completion failed")
        storeLookup <- store.lookup(transfer10).value
      } yield assert(
        storeLookup == Left(TransferCompleted(transfer10, toc)),
        s"transfer is gone from store when completeTransfer finished",
      )
    }

    "report missing transfers" in {
      val store = new InMemoryTransferStore(targetDomain, loggerFactory)
      val cache = new TransferCache(store, loggerFactory)

      for {
        missing <- cache.completeTransfer(transfer10, toc).value
      } yield {
        assert(missing == Checked.continue(UnknownReassignmentId(transfer10)))
      }
    }

    "report mismatches" in {
      val backingStore = new InMemoryTransferStore(targetDomain, loggerFactory)
      val store = new HookTransferStore(backingStore)
      val cache = new TransferCache(store, loggerFactory)
      val toc2 = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(1))
      val toc3 = TimeOfChange(RequestCounter(1), CantonTimestamp.Epoch)

      val promise = Promise[Checked[Nothing, TransferStoreError, Unit]]()

      for {
        transferData <- transferDataF
        _ <- valueOrFail(store.addTransfer(transferData).failOnShutdown)("add failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == transfer10)
          promise.completeWith(cache.completeTransfer(transfer10, toc2).value)
          CheckedT.resultT(())
        }
        _ <- valueOrFail(cache.completeTransfer(transfer10, toc))("first completion failed")
        complete3 <- cache.completeTransfer(transfer10, toc3).value
        complete2 <- promise.future
      } yield {
        assert(
          complete2 == Checked.continue(TransferAlreadyCompleted(transfer10, toc2)),
          s"second completion fails",
        )
        assert(
          complete3 == Checked.continue(TransferAlreadyCompleted(transfer10, toc3)),
          "third completion refers back to first",
        )
      }
    }

    "report mismatches coming from the store" in {
      val backingStore = new InMemoryTransferStore(targetDomain, loggerFactory)
      val store = new HookTransferStore(backingStore)
      val cache = new TransferCache(store, loggerFactory)
      val toc2 = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(1))

      val promise = Promise[Checked[Nothing, TransferStoreError, Unit]]()

      for {
        transferData <- transferDataF
        _ <- valueOrFail(store.addTransfer(transferData).failOnShutdown)("add failed")
        _ <- valueOrFail(store.completeTransfer(transfer10, toc2))("first completion failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == transfer10)
          promise.completeWith(cache.completeTransfer(transfer10, toc).value)
          CheckedT.resultT(())
        }
        complete1 <- cache.completeTransfer(transfer10, toc).value
        complete2 <- promise.future
      } yield {
        complete1.nonaborts.toList.toSet shouldBe Set(TransferAlreadyCompleted(transfer10, toc))
        complete2 shouldBe Checked.continue(TransferAlreadyCompleted(transfer10, toc))
      }
    }

    "complete only after having persisted the completion" in {
      val backingStore = new InMemoryTransferStore(targetDomain, loggerFactory)
      val store = new HookTransferStore(backingStore)
      val cache = new TransferCache(store, loggerFactory)

      val promise = Promise[Assertion]()

      for {
        transferData <- transferDataF
        _ <- valueOrFail(store.addTransfer(transferData).failOnShutdown)("add failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == transfer10)
          val f = for {
            _ <- valueOrFail(cache.completeTransfer(transfer10, toc))(
              "second completion should be idempotent"
            )
            lookup <- store.lookup(transfer10).value
          } yield lookup shouldBe Left(TransferCompleted(transfer10, toc))
          promise.completeWith(f)
          CheckedT.resultT(())
        }
        _ <- valueOrFail(cache.completeTransfer(transfer10, toc))("first completion succeeds")
        _ <- promise.future
      } yield succeed
    }

    val earlierTimestampedCompletion = toc
    val laterTimestampedCompletion =
      TimeOfChange(RequestCounter(2), CantonTimestamp.ofEpochSecond(2))

    "store the first completing request" in {
      val store = new InMemoryTransferStore(targetDomain, loggerFactory)
      val cache = new TransferCache(store, loggerFactory)

      for {
        transferData <- transferDataF
        _ <- valueOrFail(store.addTransfer(transferData).failOnShutdown)("add failed")
        _ <- valueOrFail(cache.completeTransfer(transfer10, laterTimestampedCompletion))(
          "first completion fails"
        )
        complete <- cache.completeTransfer(transfer10, earlierTimestampedCompletion).value
        lookup <- leftOrFail(store.lookup(transfer10))("lookup succeeded")
      } yield {
        complete.nonaborts shouldBe Chain(
          TransferAlreadyCompleted(transfer10, earlierTimestampedCompletion)
        )
        lookup shouldBe TransferCompleted(transfer10, laterTimestampedCompletion)
      }
    }

    "pick sensibly from concurrent completing requests" in {
      import cats.implicits.*
      implicit val ec: ExecutionContextIdlenessExecutorService = executorService

      val store = new InMemoryTransferStore(targetDomain, loggerFactory)
      val cache = new TransferCache(store, loggerFactory)(executorService)

      val timestamps = (1L to 100L).toList.map { ts =>
        TimeOfChange(RequestCounter(ts), CantonTimestamp.ofEpochSecond(ts))
      }

      def completeAndLookup(time: TimeOfChange): Future[
        (Checked[Nothing, TransferStoreError, Unit], Either[TransferLookupError, TransferData])
      ] =
        for {
          complete <- cache.completeTransfer(transfer10, time).value
          lookup <- (store.lookup(transfer10)(traceContext)).value
        } yield {
          complete -> lookup
        }

      for {
        transferData <- transferDataF
        _ <- valueOrFail(store.addTransfer(transferData).failOnShutdown)("add failed")

        resultFutures = (timestamps).map { time =>
          completeAndLookup(time)
        }
        results <- resultFutures.sequence

      } yield {
        val completions = results.map(x => x._1)
        val lookups = results.map(x => x._2)

        completions should have length timestamps.length.longValue()
        completions.count(p => p.successful) shouldBe 1

        lookups should have length timestamps.length.longValue()
        lookups.distinct should have length 1
      }
    }
  }
}

object TransferCacheTest {

  class HookTransferStore(baseStore: TransferStore)(implicit ec: ExecutionContext)
      extends TransferStore {

    private[this] val preCompleteHook: AtomicReference[
      (ReassignmentId, TimeOfChange) => CheckedT[Future, Nothing, TransferStoreError, Unit]
    ] =
      new AtomicReference(HookTransferStore.preCompleteNoHook)

    def preComplete(
        hook: (ReassignmentId, TimeOfChange) => CheckedT[Future, Nothing, TransferStoreError, Unit]
    ): Unit =
      preCompleteHook.set(hook)

    override def addTransfer(transferData: TransferData)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] =
      baseStore.addTransfer(transferData)

    override def addUnassignmentResult(unassignmentResult: DeliveredUnassignmentResult)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] =
      baseStore.addUnassignmentResult(unassignmentResult)

    override def addTransfersOffsets(
        offsets: Map[ReassignmentId, TransferData.TransferGlobalOffset]
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] =
      baseStore.addTransfersOffsets(offsets)

    override def completeTransfer(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(
        implicit traceContext: TraceContext
    ): CheckedT[Future, Nothing, TransferStoreError, Unit] = {
      val hook = preCompleteHook.getAndSet(HookTransferStore.preCompleteNoHook)
      hook(reassignmentId, timeOfCompletion).flatMap[Nothing, TransferStoreError, Unit](_ =>
        baseStore.completeTransfer(reassignmentId, timeOfCompletion)
      )
    }

    override def deleteTransfer(reassignmentId: ReassignmentId)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      baseStore.deleteTransfer(reassignmentId)

    override def deleteCompletionsSince(criterionInclusive: RequestCounter)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      baseStore.deleteCompletionsSince(criterionInclusive)

    override def find(
        filterSource: Option[SourceDomainId],
        filterTimestamp: Option[CantonTimestamp],
        filterSubmitter: Option[LfPartyId],
        limit: Int,
    )(implicit traceContext: TraceContext): Future[Seq[TransferData]] =
      baseStore.find(filterSource, filterTimestamp, filterSubmitter, limit)

    override def findAfter(requestAfter: Option[(CantonTimestamp, SourceDomainId)], limit: Int)(
        implicit traceContext: TraceContext
    ): Future[Seq[TransferData]] = baseStore.findAfter(requestAfter, limit)

    override def findIncomplete(
        sourceDomain: Option[SourceDomainId],
        validAt: GlobalOffset,
        stakeholders: Option[NonEmpty[Set[LfPartyId]]],
        limit: NonNegativeInt,
    )(implicit traceContext: TraceContext): Future[Seq[IncompleteTransferData]] =
      baseStore.findIncomplete(sourceDomain, validAt, stakeholders, limit)

    override def findEarliestIncomplete()(implicit
        traceContext: TraceContext
    ): Future[Option[(GlobalOffset, ReassignmentId, TargetDomainId)]] =
      baseStore.findEarliestIncomplete()

    override def lookup(reassignmentId: ReassignmentId)(implicit
        traceContext: TraceContext
    ): EitherT[Future, TransferLookupError, TransferData] =
      baseStore.lookup(reassignmentId)
  }

  object HookTransferStore {
    val preCompleteNoHook
        : (ReassignmentId, TimeOfChange) => CheckedT[Future, Nothing, TransferStoreError, Unit] =
      (_: ReassignmentId, _: TimeOfChange) => CheckedT(Future.successful(Checked.result(())))
  }

  class PromiseHook[A](promise: Promise[A]) extends Promise[A] {

    private[this] val futureHook: AtomicReference[() => Unit] =
      new AtomicReference[() => Unit](() => ())

    def setFutureHook(hook: () => Unit): Unit = futureHook.set(hook)

    override def future: Future[A] = {
      futureHook.getAndSet(() => ())()
      promise.future
    }

    override def isCompleted: Boolean = promise.isCompleted

    override def tryComplete(result: Try[A]): Boolean = promise.tryComplete(result)
  }
}
