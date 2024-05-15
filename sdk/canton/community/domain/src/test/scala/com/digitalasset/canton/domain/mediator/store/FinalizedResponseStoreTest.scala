// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.domain.mediator.{FinalizedResponse, MediatorVerdict}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.protocol.messages.InformeeMessage
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RequestId, RootHash}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingIdentityFactory}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{ApplicationId, BaseTest, CommandId, LfPartyId}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.UUID
import scala.concurrent.Future

trait FinalizedResponseStoreTest extends BeforeAndAfterAll {
  self: AsyncWordSpec with BaseTest =>

  def ts(n: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(n.toLong)
  def requestIdTs(n: Int): RequestId = RequestId(ts(n))

  val requestId = RequestId(CantonTimestamp.Epoch)
  val fullInformeeTree = {
    val domainId = DefaultTestIdentities.domainId
    val participantId = DefaultTestIdentities.participant1

    val alice = LfPartyId.assertFromString("alice")
    val aliceInformee = PlainInformee(alice)
    val bobConfirmingParty = ConfirmingParty(
      LfPartyId.assertFromString("bob"),
      PositiveInt.tryCreate(2),
    )
    val hashOps = new SymbolicPureCrypto

    def h(i: Int): Hash = TestHash.digest(i)
    def rh(index: Int): RootHash = RootHash(h(index))
    def s(i: Int): Salt = TestSalt.generateSalt(i)

    val viewCommonData =
      ViewCommonData.create(hashOps)(
        Set(aliceInformee, bobConfirmingParty),
        NonNegativeInt.tryCreate(2),
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
      hashOps,
      testedProtocolVersion,
    )
    val commonMetadata = CommonMetadata
      .create(hashOps, testedProtocolVersion)(
        ConfirmationPolicy.Signatory,
        domainId,
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
  val informeeMessage =
    InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
  val currentVersion = FinalizedResponse(
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

  val pureCryptoApi: CryptoPureApi = TestingIdentityFactory.pureCrypto()

  def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table med_response_aggregations", functionFullName)
  }
  "DbFinalizedResponseStore" should {
    behave like finalizedResponseStore(() =>
      new DbFinalizedResponseStore(
        storage,
        pureCryptoApi,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class FinalizedResponseStoreTestH2 extends DbFinalizedResponseStoreTest with H2Test

class FinalizedResponseStoreTestPostgres extends DbFinalizedResponseStoreTest with PostgresTest
