// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import cats.Monad
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.protocol.messages.InformeeMessage
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.mediator.{FinalizedResponse, MediatorVerdict}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{ApplicationId, BaseTest, CommandId, LfPartyId}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.time.Duration
import java.util.UUID

trait FinalizedResponseStoreTest extends BeforeAndAfterAll {
  self: AsyncWordSpec with BaseTest =>

  def ts(n: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(n.toLong)
  def requestIdTs(n: Int): RequestId = RequestId(ts(n))

  val requestId: RequestId = RequestId(CantonTimestamp.Epoch)
  val fullInformeeTree: FullInformeeTree = {
    val synchronizerId = DefaultTestIdentities.synchronizerId
    val participantId = DefaultTestIdentities.participant1

    val alice = LfPartyId.assertFromString("alice")
    val bob = LfPartyId.assertFromString("bob")
    val bobCp = Map(bob -> PositiveInt.tryCreate(2))
    val hashOps = new SymbolicPureCrypto

    def h(i: Int): Hash = TestHash.digest(i)
    def rh(index: Int): RootHash = RootHash(h(index))
    def s(i: Int): Salt = TestSalt.generateSalt(i)

    val viewCommonData =
      ViewCommonData.tryCreate(hashOps)(
        ViewConfirmationParameters.tryCreate(
          Set(alice, bob),
          Seq(Quorum(bobCp, NonNegativeInt.tryCreate(2))),
        ),
        s(999),
        testedProtocolVersion,
      )
    val view = TransactionView.tryCreate(hashOps)(
      viewCommonData,
      BlindedNode(rh(0)),
      TransactionSubviews.empty(testedProtocolVersion, hashOps),
      testedProtocolVersion,
    )
    val submitterMetadata = SubmitterMetadata(
      NonEmpty(Set, alice),
      ApplicationId.assertFromString("kaese"),
      CommandId.assertFromString("wurst"),
      participantId,
      salt = s(6638),
      None,
      DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      CantonTimestamp.MaxValue,
      None,
      hashOps,
      testedProtocolVersion,
    )
    val commonMetadata = CommonMetadata
      .create(hashOps, testedProtocolVersion)(
        synchronizerId,
        MediatorGroupRecipient(MediatorGroupIndex.zero),
        s(5417),
        new UUID(0L, 0L),
      )
    FullInformeeTree.tryCreate(
      GenTransactionTree.tryCreate(hashOps)(
        submitterMetadata,
        commonMetadata,
        BlindedNode(rh(12)),
        MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view :: Nil),
      ),
      testedProtocolVersion,
    )
  }
  val informeeMessage: InformeeMessage =
    InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
  val currentVersion: FinalizedResponse = FinalizedResponse(
    requestId,
    informeeMessage,
    requestId.unwrap,
    MediatorVerdict
      .MediatorReject(
        MediatorError.Timeout.Reject(
          unresponsiveParties = DefaultTestIdentities.party1.toLf
        )
      )
      .toVerdict(testedProtocolVersion),
  )(TraceContext.empty)

  private[mediator] def finalizedResponseStore(mk: () => FinalizedResponseStore): Unit = {
    implicit val closeContext: CloseContext = HasTestCloseContext.makeTestCloseContext(self.logger)

    "when storing responses" should {
      "get error message if trying to fetch a non existing response" in {
        val sut = mk()
        sut.fetch(requestId).value.map { result =>
          result shouldBe None
        }
      }.failOnShutdown("Unexpected shutdown.")
      "should be able to fetch previously stored response" in {
        val sut = mk()
        for {
          _ <- sut.store(currentVersion)
          result <- sut.fetch(requestId).value
        } yield result shouldBe Some(currentVersion)
      }.failOnShutdown("Unexpected shutdown.")
      "should allow the same response to be stored more than once" in {
        // can happen after a crash and event replay
        val sut = mk()
        for {
          _ <- sut.store(currentVersion)
          _ <- sut.store(currentVersion)
        } yield succeed
      }.failOnShutdown("Unexpected shutdown.")
    }

    "when reading events" should {
      val responses = (1 to 10).map { i =>
        currentVersion.copy(
          RequestId(CantonTimestamp.Epoch.plusSeconds(i.toLong)),
          // simulate that the verdicts where issued in the reverse order: oldest request confirmed first.
          // also causes 2 verdicts to have the same version timestamp, so we can confirm that the store
          // returns them in the proper order
          finalizationTime = CantonTimestamp.Epoch.plusSeconds(20 - (i.toLong / 2)),
        )(currentVersion.requestTraceContext)
      }.toList

      val responsesInExpectedOrder = responses.sortBy(r => (r.version, r.requestId.unwrap))

      "sort in order of (version, request_id)" in {
        val sut = mk()
        for {
          _ <- MonadUtil.sequentialTraverse_(responses)(sut.store(_))
          loadedVerdicts <- sut.readFinalizedVerdicts(
            fromFinalizationTimeExclusive = CantonTimestamp.MinValue,
            fromRequestExclusive = CantonTimestamp.MinValue,
            toFinalizationTimeInclusive = CantonTimestamp.MaxValue,
            PositiveInt.MaxValue,
          )
        } yield {
          loadedVerdicts should contain theSameElementsInOrderAs responsesInExpectedOrder
        }
      }.failOnShutdown("unexpected shutdown.")

      "respect the batch size" in {
        val sut = mk()
        for {
          _ <- MonadUtil.sequentialTraverse_(responses)(sut.store(_))
          loadedVerdicts <- sut.readFinalizedVerdicts(
            fromFinalizationTimeExclusive = CantonTimestamp.MinValue,
            fromRequestExclusive = CantonTimestamp.MinValue,
            toFinalizationTimeInclusive = CantonTimestamp.MaxValue,
            PositiveInt.two,
          )
        } yield {
          loadedVerdicts should contain theSameElementsInOrderAs responsesInExpectedOrder.take(2)
        }
      }.failOnShutdown("unexpected shutdown.")

      "read a batch correctly when starting in the 'middle' of a version timestamp" in {
        val sut = mk()
        // test with multiple batch sizes
        val batchSizes = Seq(3, 4)

        MonadUtil
          .sequentialTraverse[Int, FutureUnlessShutdown, Assertion](batchSizes) { batchSize =>
            for {
              _ <- MonadUtil.sequentialTraverse_(responses)(sut.store(_))

              // tailRecM re-runs the effect until it returns Left with the result.
              // The result of Right(x) is passed as the input for the next iteration
              // of the effect.
              // In this case, we continue to load up to batchSize number of verdicts
              // as long as we find verdicts, returning the timestamps of the last verdict
              // in the batch as the result of the effect, which then serve as the input
              // for the next iteration's lookup.
              // If no verdicts were found, batch.lasOption.toLeft(acc) will return the
              // accumulated verdicts.
              batches <- Monad[FutureUnlessShutdown].tailRecM(
                (
                  /* resulting batches = */ Vector.empty[Seq[FinalizedResponse]],
                  /* fromVersionExclusive = */ CantonTimestamp.MinValue,
                  /* fromRequestExclusive = */ CantonTimestamp.MinValue,
                )
              ) { case (acc, fromVersionExclusive, fromResultExclusive) =>
                sut
                  .readFinalizedVerdicts(
                    fromFinalizationTimeExclusive = fromVersionExclusive,
                    fromRequestExclusive = fromResultExclusive,
                    toFinalizationTimeInclusive = CantonTimestamp.MaxValue,
                    PositiveInt.tryCreate(batchSize),
                  )
                  .map { batch =>
                    batch.lastOption
                      .map { latestReceivedVerdict =>
                        val nextFromVersionExclusive = latestReceivedVerdict.version
                        val nextFromRequestExclusive = latestReceivedVerdict.requestId.unwrap
                        (acc :+ batch, nextFromVersionExclusive, nextFromRequestExclusive)
                      }
                      .toLeft(acc)
                  }
              }

            } yield {
              // check that each individual batch against the expected order
              forAll(batches.zipWithIndex) { case (batch, index) =>
                batch should contain theSameElementsInOrderAs responsesInExpectedOrder.slice(
                  batchSize * index,
                  batchSize * index + batchSize,
                )
              }
              // slightly superfluous, but also check that the concatenated batches contain all expected elements
              batches.flatten should contain theSameElementsInOrderAs responsesInExpectedOrder
            }
          }
          .failOnShutdown("unexpected shutdown.")
          .map(_ => succeed)
      }

      "return an empty list if there are no results" in {
        val sut = mk()
        for {
          loadedVerdictsOnEmptyStore <- sut.readFinalizedVerdicts(
            fromFinalizationTimeExclusive = CantonTimestamp.MinValue,
            fromRequestExclusive = CantonTimestamp.MinValue,
            toFinalizationTimeInclusive = CantonTimestamp.MaxValue,
            PositiveInt.two,
          )
          _ <- MonadUtil.sequentialTraverse_(responses)(sut.store(_))
          loadedVerdicts <- sut.readFinalizedVerdicts(
            fromFinalizationTimeExclusive = CantonTimestamp.MaxValue.minusSeconds(1),
            fromRequestExclusive = CantonTimestamp.MaxValue.minusSeconds(1),
            toFinalizationTimeInclusive = CantonTimestamp.MaxValue,
            PositiveInt.two,
          )

        } yield {
          loadedVerdictsOnEmptyStore shouldBe empty
          loadedVerdicts shouldBe empty
        }
      }.failOnShutdown("unexpected shutdown.")

    }

    "pruning" should {
      "remove all responses up and including timestamp" in {
        val sut = mk()

        val requests =
          (1 to 3).map(n => currentVersion.copy(requestId = requestIdTs(n))(TraceContext.empty))

        for {
          _ <- requests.toList.parTraverse(sut.store)
          _ <- sut.prune(ts(2))
          _ <- noneOrFailUS(sut.fetch(requestIdTs(1)))("fetch(ts1)")
          _ <- noneOrFailUS(sut.fetch(requestIdTs(2)))("fetch(ts2)")
          _ <- valueOrFailUS(sut.fetch(requestIdTs(3)))("fetch(ts3)")
        } yield succeed
      }.failOnShutdown("Unexpected shutdown.")
    }
  }
}

class FinalizedResponseStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with FinalizedResponseStoreTest {
  "InMemoryFinalizedResponseStore" should {
    behave like finalizedResponseStore(() => new InMemoryFinalizedResponseStore(loggerFactory))
  }
}

trait DbFinalizedResponseStoreTest
    extends AsyncWordSpec
    with BaseTest
    with FinalizedResponseStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table med_response_aggregations", functionFullName)
  }
  "DbFinalizedResponseStore" should {
    behave like finalizedResponseStore(() =>
      new DbFinalizedResponseStore(
        storage,
        new SymbolicPureCrypto,
        testedProtocolVersion,
        CachingConfigs.defaultFinalizedMediatorConfirmationRequestsCache,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class FinalizedResponseStoreTestH2 extends DbFinalizedResponseStoreTest with H2Test

class FinalizedResponseStoreTestPostgres extends DbFinalizedResponseStoreTest with PostgresTest
