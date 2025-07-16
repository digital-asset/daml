// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{Chain, EitherT}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset, UnassignmentData}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.participant.store.memory.ReassignmentCacheTest.HookReassignmentStore
import com.digitalasset.canton.participant.store.{ReassignmentStore, ReassignmentStoreTest}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.{BaseTest, HasExecutorService, LfPartyId}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

final class ReassignmentCacheTest extends AsyncWordSpec with BaseTest with HasExecutorService {
  import ReassignmentStoreTest.*

  private val reassignmentData =
    mkUnassignmentDataForSynchronizer(
      mediator1,
      sourceSynchronizerId = sourceSynchronizer1,
      targetSynchronizerId = targetSynchronizerId,
    )
  private val ts = CantonTimestamp.Epoch

  private def createStore: InMemoryReassignmentStore =
    new InMemoryReassignmentStore(targetSynchronizerId, loggerFactory)

  "find reassignments in the backing store" in {
    val store = createStore
    val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

    for {
      _ <- valueOrFail(store.addUnassignmentData(reassignmentData).failOnShutdown)("add failed")
      _ <- valueOrFail(store.lookup(reassignmentData.reassignmentId).failOnShutdown)(
        "lookup did not find reassignment"
      )
      lookup11 <- cache.lookup(reassignment11).value.failOnShutdown
      _ <- store.deleteReassignment(reassignmentData.reassignmentId).failOnShutdown
      deleted <- cache.lookup(reassignmentData.reassignmentId).value.failOnShutdown
    } yield {
      lookup11 shouldBe Left(UnknownReassignmentId(reassignment11))
      deleted shouldBe Left(UnknownReassignmentId(reassignmentData.reassignmentId))
    }
  }

  "completeReassignment" should {
    "immediately report the reassignment as completed" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)
      for {
        _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignmentData.reassignmentId)
          CheckedT(
            cache.lookup(reassignmentData.reassignmentId).value.failOnShutdown.map {
              case Left(ReassignmentCompleted(id, _toc)) if id == reassignmentData.reassignmentId =>
                Checked.result(())
              case result => fail(s"Invalid lookup result $result")
            }
          )
        }
        _ <- valueOrFail(cache.completeReassignment(reassignmentData.reassignmentId, ts))(
          "first completion failed"
        )
        storeLookup <- store.lookup(reassignmentData.reassignmentId).value
      } yield assert(
        storeLookup == Left(ReassignmentCompleted(reassignmentData.reassignmentId, ts)),
        s"reassignment is gone from store when completeReassignment finished",
      )
    }.failOnShutdown

    "report missing reassignments" in {
      val store = createStore
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

      for {
        missing <- cache
          .completeReassignment(reassignmentData.reassignmentId, ts)
          .value
          .failOnShutdown
      } yield {
        assert(missing == Checked.continue(UnknownReassignmentId(reassignmentData.reassignmentId)))
      }
    }

    "report mismatches" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)
      val ts2 = CantonTimestamp.ofEpochSecond(2)
      val ts3 = CantonTimestamp.ofEpochSecond(1)

      val promise = Promise[Checked[Nothing, ReassignmentStoreError, Unit]]()

      for {
        _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed").failOnShutdown
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignmentData.reassignmentId)
          promise.completeWith(
            cache.completeReassignment(reassignmentData.reassignmentId, ts2).value.failOnShutdown
          )
          CheckedT.resultT(())
        }
        _ <- valueOrFail(cache.completeReassignment(reassignmentData.reassignmentId, ts))(
          "first completion failed"
        ).failOnShutdown
        complete3 <- cache
          .completeReassignment(reassignmentData.reassignmentId, ts3)
          .value
          .failOnShutdown
        complete2 <- promise.future
      } yield {
        assert(
          complete2 == Checked.continue(
            ReassignmentAlreadyCompleted(reassignmentData.reassignmentId, ts2)
          ),
          s"second completion fails",
        )
        assert(
          complete3 == Checked.continue(
            ReassignmentAlreadyCompleted(reassignmentData.reassignmentId, ts3)
          ),
          "third completion refers back to first",
        )
      }
    }

    "report mismatches coming from the store" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)
      val ts2 = CantonTimestamp.ofEpochSecond(1)

      val promise = Promise[Checked[Nothing, ReassignmentStoreError, Unit]]()

      for {
        _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
        _ <- valueOrFail(store.completeReassignment(reassignmentData.reassignmentId, ts2))(
          "first completion failed"
        )
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignmentData.reassignmentId)
          promise.completeWith(
            cache.completeReassignment(reassignmentData.reassignmentId, ts).value.failOnShutdown
          )
          CheckedT.resultT(())
        }
        complete1 <- cache.completeReassignment(reassignmentData.reassignmentId, ts).value
        complete2 <- FutureUnlessShutdown.outcomeF(promise.future)
      } yield {
        complete1.nonaborts.toList.toSet shouldBe Set(
          ReassignmentAlreadyCompleted(reassignmentData.reassignmentId, ts)
        )
        complete2 shouldBe Checked.continue(
          ReassignmentAlreadyCompleted(reassignmentData.reassignmentId, ts)
        )
      }
    }.failOnShutdown

    "complete only after having persisted the completion" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

      val promise = Promise[Assertion]()

      for {
        _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignmentData.reassignmentId)
          val f = for {
            _ <- valueOrFail(cache.completeReassignment(reassignmentData.reassignmentId, ts))(
              "second completion should be idempotent"
            )
            lookup <- store.lookup(reassignmentData.reassignmentId).value
          } yield lookup shouldBe Left(ReassignmentCompleted(reassignmentData.reassignmentId, ts))
          promise.completeWith(f.failOnShutdown)
          CheckedT.resultT(())
        }
        _ <- valueOrFail(cache.completeReassignment(reassignmentData.reassignmentId, ts))(
          "first completion succeeds"
        )
        _ <- FutureUnlessShutdown.outcomeF(promise.future)
      } yield succeed
    }.failOnShutdown

    val earlierTimestampedCompletion = ts
    val laterTimestampedCompletion = CantonTimestamp.ofEpochSecond(2)

    "store the first completing request" in {
      val store = createStore
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

      for {
        _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
        _ <- valueOrFail(
          cache.completeReassignment(reassignmentData.reassignmentId, laterTimestampedCompletion)
        )(
          "first completion fails"
        )
        complete <- cache
          .completeReassignment(reassignmentData.reassignmentId, earlierTimestampedCompletion)
          .value
        lookup <- leftOrFail(store.lookup(reassignmentData.reassignmentId))("lookup succeeded")
      } yield {
        complete.nonaborts shouldBe Chain(
          ReassignmentAlreadyCompleted(
            reassignmentData.reassignmentId,
            earlierTimestampedCompletion,
          )
        )
        lookup shouldBe ReassignmentCompleted(
          reassignmentData.reassignmentId,
          laterTimestampedCompletion,
        )
      }
    }.failOnShutdown

    "pick sensibly from concurrent completing requests" in {
      import cats.implicits.*
      implicit val ec: ExecutionContextIdlenessExecutorService = executorService

      val store = createStore
      val cache =
        new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)(
          executorService
        )

      val timestamps = (1L to 100L).toList.map(CantonTimestamp.ofEpochSecond)

      def completeAndLookup(ts: CantonTimestamp): Future[
        (
            Checked[Nothing, ReassignmentStoreError, Unit],
            Either[ReassignmentLookupError, UnassignmentData],
        )
      ] =
        for {
          complete <- cache
            .completeReassignment(reassignmentData.reassignmentId, ts)
            .value
            .failOnShutdown
          lookup <- (store
            .lookup(reassignmentData.reassignmentId)(traceContext))
            .value
            .failOnShutdown
        } yield {
          complete -> lookup
        }

      for {
        _ <- valueOrFail(store.addUnassignmentData(reassignmentData).failOnShutdown)("add failed")

        resultFutures = (timestamps).map { ts =>
          completeAndLookup(ts)
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

object ReassignmentCacheTest extends BaseTest {

  class HookReassignmentStore(baseStore: ReassignmentStore)(implicit ec: ExecutionContext)
      extends ReassignmentStore {

    private[this] val preCompleteHook: AtomicReference[
      (ReassignmentId, CantonTimestamp) => CheckedT[Future, Nothing, ReassignmentStoreError, Unit]
    ] =
      new AtomicReference(HookReassignmentStore.preCompleteNoHook)

    def preComplete(
        hook: (
            ReassignmentId,
            CantonTimestamp,
        ) => CheckedT[Future, Nothing, ReassignmentStoreError, Unit]
    ): Unit =
      preCompleteHook.set(hook)

    override def addUnassignmentData(unassignmentData: UnassignmentData)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
      baseStore.addUnassignmentData(unassignmentData)

    override def addReassignmentsOffsets(
        offsets: Map[ReassignmentId, UnassignmentData.ReassignmentGlobalOffset]
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
      baseStore.addReassignmentsOffsets(offsets)

    override def completeReassignment(
        reassignmentId: ReassignmentId,
        tsCompletion: CantonTimestamp,
    )(implicit
        traceContext: TraceContext
    ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] = {
      val hook = preCompleteHook.getAndSet(HookReassignmentStore.preCompleteNoHook)
      hook(reassignmentId, tsCompletion)
        .mapK(FutureUnlessShutdown.outcomeK)
        .flatMap[Nothing, ReassignmentStoreError, Unit](_ =>
          baseStore.completeReassignment(reassignmentId, tsCompletion)
        )
    }

    override def deleteReassignment(reassignmentId: ReassignmentId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] =
      baseStore.deleteReassignment(reassignmentId)

    override def deleteCompletionsSince(criterionInclusive: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] =
      baseStore.deleteCompletionsSince(criterionInclusive)

    override def addAssignmentDataIfAbsent(assignmentData: AssignmentData)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
      baseStore.addAssignmentDataIfAbsent(assignmentData)

    override def findAfter(
        requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])],
        limit: Int,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[UnassignmentData]] = baseStore.findAfter(requestAfter, limit)

    override def findIncomplete(
        sourceSynchronizer: Option[Source[SynchronizerId]],
        validAt: Offset,
        stakeholders: Option[NonEmpty[Set[LfPartyId]]],
        limit: NonNegativeInt,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]] =
      baseStore.findIncomplete(sourceSynchronizer, validAt, stakeholders, limit)

    override def findEarliestIncomplete()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]] =
      baseStore.findEarliestIncomplete()

    override def lookup(reassignmentId: ReassignmentId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentLookupError, UnassignmentData] =
      baseStore.lookup(reassignmentId)

    override def findContractReassignmentId(
        contractIds: Seq[LfContractId],
        sourceSynchronizer: Option[Source[SynchronizerId]],
        unassignmentTs: Option[CantonTimestamp],
        completionTs: Option[CantonTimestamp],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]] =
      baseStore.findContractReassignmentId(
        contractIds,
        sourceSynchronizer,
        unassignmentTs,
        completionTs,
      )

    override def findReassignmentEntry(reassignmentId: ReassignmentId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, UnknownReassignmentId, ReassignmentEntry] =
      baseStore.findReassignmentEntry(reassignmentId)

    override def listInFlightReassignmentIds()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[ReassignmentId]] = baseStore.listInFlightReassignmentIds()

  }

  object HookReassignmentStore {
    val preCompleteNoHook: (
        ReassignmentId,
        CantonTimestamp,
    ) => CheckedT[Future, Nothing, ReassignmentStoreError, Unit] =
      (_: ReassignmentId, _: CantonTimestamp) => CheckedT(Future.successful(Checked.result(())))
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
