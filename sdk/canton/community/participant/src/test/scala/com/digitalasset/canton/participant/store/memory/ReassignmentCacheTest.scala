// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{Chain, EitherT}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.{
  IncompleteReassignmentData,
  ReassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.participant.store.memory.ReassignmentCacheTest.HookReassignmentStore
import com.digitalasset.canton.participant.store.{ReassignmentStore, ReassignmentStoreTest}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.{BaseTest, HasExecutorService, LfPartyId, RequestCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class ReassignmentCacheTest extends AsyncWordSpec with BaseTest with HasExecutorService {
  import ReassignmentStoreTest.*

  private val reassignmentData =
    mkReassignmentDataForDomain(
      reassignment10,
      mediator1,
      targetDomainId = targetDomainId,
    )
  private val toc = TimeOfChange(RequestCounter(0), CantonTimestamp.Epoch)

  private def createStore: InMemoryReassignmentStore =
    new InMemoryReassignmentStore(targetDomainId, loggerFactory)

  "find reassignments in the backing store" in {
    val store = createStore
    val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

    for {
      _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
      _ <- valueOrFail(store.lookup(reassignment10).failOnShutdown)(
        "lookup did not find reassignment"
      )
      lookup11 <- cache.lookup(reassignment11).value.failOnShutdown
      _ <- store.deleteReassignment(reassignment10).failOnShutdown
      deleted <- cache.lookup(reassignment10).value.failOnShutdown
    } yield {
      lookup11 shouldBe Left(UnknownReassignmentId(reassignment11))
      deleted shouldBe Left(UnknownReassignmentId(reassignment10))
    }
  }

  "completeReassignment" should {
    "immediately report the reassignment as completed" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)
      for {
        _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignment10)
          CheckedT(
            cache.lookup(reassignment10).value.failOnShutdown.map {
              case Left(ReassignmentCompleted(`reassignment10`, `toc`)) => Checked.result(())
              case result => fail(s"Invalid lookup result $result")
            }
          )
        }
        _ <- valueOrFail(cache.completeReassignment(reassignment10, toc))("first completion failed")
        storeLookup <- store.lookup(reassignment10).value
      } yield assert(
        storeLookup == Left(ReassignmentCompleted(reassignment10, toc)),
        s"reassignment is gone from store when completeReassignment finished",
      )
    }.failOnShutdown

    "report missing reassignments" in {
      val store = createStore
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

      for {
        missing <- cache.completeReassignment(reassignment10, toc).value.failOnShutdown
      } yield {
        assert(missing == Checked.continue(UnknownReassignmentId(reassignment10)))
      }
    }

    "report mismatches" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)
      val toc2 = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(1))
      val toc3 = TimeOfChange(RequestCounter(1), CantonTimestamp.Epoch)

      val promise = Promise[Checked[Nothing, ReassignmentStoreError, Unit]]()

      for {
        _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed").failOnShutdown
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignment10)
          promise.completeWith(
            cache.completeReassignment(reassignment10, toc2).value.failOnShutdown
          )
          CheckedT.resultT(())
        }
        _ <- valueOrFail(cache.completeReassignment(reassignment10, toc))(
          "first completion failed"
        ).failOnShutdown
        complete3 <- cache.completeReassignment(reassignment10, toc3).value.failOnShutdown
        complete2 <- promise.future
      } yield {
        assert(
          complete2 == Checked.continue(ReassignmentAlreadyCompleted(reassignment10, toc2)),
          s"second completion fails",
        )
        assert(
          complete3 == Checked.continue(ReassignmentAlreadyCompleted(reassignment10, toc3)),
          "third completion refers back to first",
        )
      }
    }

    "report mismatches coming from the store" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)
      val toc2 = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(1))

      val promise = Promise[Checked[Nothing, ReassignmentStoreError, Unit]]()

      for {
        _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
        _ <- valueOrFail(store.completeReassignment(reassignment10, toc2))(
          "first completion failed"
        )
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignment10)
          promise.completeWith(cache.completeReassignment(reassignment10, toc).value.failOnShutdown)
          CheckedT.resultT(())
        }
        complete1 <- cache.completeReassignment(reassignment10, toc).value
        complete2 <- FutureUnlessShutdown.outcomeF(promise.future)
      } yield {
        complete1.nonaborts.toList.toSet shouldBe Set(
          ReassignmentAlreadyCompleted(reassignment10, toc)
        )
        complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(reassignment10, toc))
      }
    }.failOnShutdown

    "complete only after having persisted the completion" in {
      val backingStore = createStore
      val store = new HookReassignmentStore(backingStore)
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

      val promise = Promise[Assertion]()

      for {
        _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
        _ = store.preComplete { (reassignmentId, _) =>
          assert(reassignmentId == reassignment10)
          val f = for {
            _ <- valueOrFail(cache.completeReassignment(reassignment10, toc))(
              "second completion should be idempotent"
            )
            lookup <- store.lookup(reassignment10).value
          } yield lookup shouldBe Left(ReassignmentCompleted(reassignment10, toc))
          promise.completeWith(f.failOnShutdown)
          CheckedT.resultT(())
        }
        _ <- valueOrFail(cache.completeReassignment(reassignment10, toc))(
          "first completion succeeds"
        )
        _ <- FutureUnlessShutdown.outcomeF(promise.future)
      } yield succeed
    }.failOnShutdown

    val earlierTimestampedCompletion = toc
    val laterTimestampedCompletion =
      TimeOfChange(RequestCounter(2), CantonTimestamp.ofEpochSecond(2))

    "store the first completing request" in {
      val store = createStore
      val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)

      for {
        _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
        _ <- valueOrFail(cache.completeReassignment(reassignment10, laterTimestampedCompletion))(
          "first completion fails"
        )
        complete <- cache.completeReassignment(reassignment10, earlierTimestampedCompletion).value
        lookup <- leftOrFail(store.lookup(reassignment10))("lookup succeeded")
      } yield {
        complete.nonaborts shouldBe Chain(
          ReassignmentAlreadyCompleted(reassignment10, earlierTimestampedCompletion)
        )
        lookup shouldBe ReassignmentCompleted(reassignment10, laterTimestampedCompletion)
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

      val timestamps = (1L to 100L).toList.map { ts =>
        TimeOfChange(RequestCounter(ts), CantonTimestamp.ofEpochSecond(ts))
      }

      def completeAndLookup(time: TimeOfChange): Future[
        (
            Checked[Nothing, ReassignmentStoreError, Unit],
            Either[ReassignmentLookupError, ReassignmentData],
        )
      ] =
        for {
          complete <- cache.completeReassignment(reassignment10, time).value.failOnShutdown
          lookup <- (store.lookup(reassignment10)(traceContext)).value.failOnShutdown
        } yield {
          complete -> lookup
        }

      for {
        _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")

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

object ReassignmentCacheTest extends BaseTest {

  class HookReassignmentStore(baseStore: ReassignmentStore)(implicit ec: ExecutionContext)
      extends ReassignmentStore {

    private[this] val preCompleteHook: AtomicReference[
      (ReassignmentId, TimeOfChange) => CheckedT[Future, Nothing, ReassignmentStoreError, Unit]
    ] =
      new AtomicReference(HookReassignmentStore.preCompleteNoHook)

    def preComplete(
        hook: (
            ReassignmentId,
            TimeOfChange,
        ) => CheckedT[Future, Nothing, ReassignmentStoreError, Unit]
    ): Unit =
      preCompleteHook.set(hook)

    override def addReassignment(reassignmentData: ReassignmentData)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
      baseStore.addReassignment(reassignmentData)

    override def addUnassignmentResult(unassignmentResult: DeliveredUnassignmentResult)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
      baseStore.addUnassignmentResult(unassignmentResult)

    override def addReassignmentsOffsets(
        offsets: Map[ReassignmentId, ReassignmentData.ReassignmentGlobalOffset]
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
      baseStore.addReassignmentsOffsets(offsets)

    override def completeReassignment(
        reassignmentId: ReassignmentId,
        timeOfCompletion: TimeOfChange,
    )(implicit
        traceContext: TraceContext
    ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] = {
      val hook = preCompleteHook.getAndSet(HookReassignmentStore.preCompleteNoHook)
      hook(reassignmentId, timeOfCompletion)
        .mapK(FutureUnlessShutdown.outcomeK)
        .flatMap[Nothing, ReassignmentStoreError, Unit](_ =>
          baseStore.completeReassignment(reassignmentId, timeOfCompletion)
        )
    }

    override def deleteReassignment(reassignmentId: ReassignmentId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] =
      baseStore.deleteReassignment(reassignmentId)

    override def deleteCompletionsSince(criterionInclusive: RequestCounter)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      baseStore.deleteCompletionsSince(criterionInclusive)

    override def find(
        filterSource: Option[Source[DomainId]],
        filterTimestamp: Option[CantonTimestamp],
        filterSubmitter: Option[LfPartyId],
        limit: Int,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentData]] =
      baseStore.find(filterSource, filterTimestamp, filterSubmitter, limit)

    override def findAfter(requestAfter: Option[(CantonTimestamp, Source[DomainId])], limit: Int)(
        implicit traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[ReassignmentData]] = baseStore.findAfter(requestAfter, limit)

    override def findIncomplete(
        sourceDomain: Option[Source[DomainId]],
        validAt: Offset,
        stakeholders: Option[NonEmpty[Set[LfPartyId]]],
        limit: NonNegativeInt,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]] =
      baseStore.findIncomplete(sourceDomain, validAt, stakeholders, limit)

    override def findEarliestIncomplete()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[DomainId])]] =
      baseStore.findEarliestIncomplete()

    override def lookup(reassignmentId: ReassignmentId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ReassignmentLookupError, ReassignmentData] =
      baseStore.lookup(reassignmentId)

    override def findContractReassignmentId(
        contractIds: Seq[LfContractId],
        sourceDomain: Option[Source[DomainId]],
        unassignmentTs: Option[CantonTimestamp],
        completionTs: Option[CantonTimestamp],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]] =
      baseStore.findContractReassignmentId(
        contractIds,
        sourceDomain,
        unassignmentTs,
        completionTs,
      )
  }

  object HookReassignmentStore {
    val preCompleteNoHook: (
        ReassignmentId,
        TimeOfChange,
    ) => CheckedT[Future, Nothing, ReassignmentStoreError, Unit] =
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
