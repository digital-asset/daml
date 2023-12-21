// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.{LogEntry, TracedLogger}
import com.digitalasset.canton.protocol.messages.DomainTopologyTransactionMessage
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.TopologyManagerAlarm
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  NamespaceDelegation,
  OwnerToKeyMapping,
  ParticipantPermission,
  ParticipantState,
  RequestSide,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TrustLevel,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  TestingIdentityFactory,
  TestingTopologyTransactionFactory,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, Outcome}

class DomainTopologyTransactionMessageValidatorTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {

  override type FixtureParam = Env
  class Env extends FlagCloseable with HasCloseContext {
    import DefaultTestIdentities.*
    val mgrCrypto = TestingIdentityFactory.newCrypto(loggerFactory)(
      domainManager,
      Seq(Fingerprint.tryCreate("default")),
      Seq("dmgr", "seq"),
    )
    val clock = mock[Clock]
    val store =
      new InMemoryTopologyStore(DomainStore(domainId), loggerFactory, timeouts, futureSupervisor)

    val (mgrTopoClient, mgrCryptoClient) = TestingIdentityFactory.domainClientForOwner(
      domainManager,
      domainId,
      store,
      clock,
      testedProtocolVersion,
      futureSupervisor,
      loggerFactory,
      cryptoO = Some(mgrCrypto),
      useStateTxs = false,
    )
    val (parTopoClient, parCryptoClient) = TestingIdentityFactory.domainClientForOwner(
      participant1,
      domainId,
      store,
      clock,
      testedProtocolVersion,
      futureSupervisor,
      loggerFactory,
      useStateTxs = false,
    )

    val validator = new DomainTopologyTransactionMessageValidator.Impl(
      mgrCryptoClient,
      participant1,
      testedProtocolVersion,
      ProcessingTimeout(),
      futureSupervisor,
      loggerFactory,
    )
    object Txs extends TestingTopologyTransactionFactory {
      val mgrSigningKey = genSignKey("mgrsigning")
      val seqSigningKey = genSignKey("seqsigning")
      val pidNamespace = genSignKey(
        participant1.uid.namespace.fingerprint.unwrap
      )
      override def crypto: Crypto = mgrCrypto
      override val defaultSigningKey: SigningPublicKey =
        crypto.cryptoPublicStore
          .signingKey(domainId.uid.namespace.fingerprint)
          .futureValue
          .valueOrFail("must have key")
      val dNs = mkAdd(
        NamespaceDelegation(domainId.uid.namespace, defaultSigningKey, isRootDelegation = true)
      )
      val okmMgr = mkAdd(OwnerToKeyMapping(domainManager, mgrSigningKey))
      val okmSeq = mkAdd(OwnerToKeyMapping(sequencerId, mgrSigningKey))
      val okmMed = mkAdd(OwnerToKeyMapping(mediator, mgrSigningKey))
      val trustCert = mkAdd(
        ParticipantState(
          RequestSide.From,
          domainId,
          participant1,
          ParticipantPermission.Submission,
          TrustLevel.Ordinary,
        )
      )

      val genesis = List(dNs, okmMgr, okmSeq, trustCert)

      append(ts1, genesis)
      // update manager side to know about the state
      mgrTopoClient.updateHead(EffectiveTime(ts1), ApproximateTime(ts1), true)
    }

    def append(ts: CantonTimestamp, txs: Seq[GenericSignedTopologyTransaction]): Unit = {
      store
        .append(
          SequencedTime(ts),
          EffectiveTime(ts),
          txs.map(ValidatedTopologyTransaction(_, None)),
        )
        .futureValue
    }

    def build(ts: CantonTimestamp, txs: List[GenericSignedTopologyTransaction]) =
      DomainTopologyTransactionMessage
        .tryCreate(
          txs,
          mgrCryptoClient.headSnapshot,
          DefaultTestIdentities.domainId,
          ts.plusMillis(10),
          testedProtocolVersion,
        )
        .futureValue

    def inject(
        ts: CantonTimestamp,
        message: DomainTopologyTransactionMessage,
    ): List[SignedTopologyTransaction[TopologyChangeOp]] = {
      validator
        .extractTopologyUpdatesAndValidateEnvelope(
          SequencedTime(ts),
          List(
            OpenEnvelope(message, Recipients.cc(participant1))(
              testedProtocolVersion
            )
          ),
        )
        .onShutdown(fail("test should not hit a shutdown"))
        .futureValue
    }
    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    override protected def logger: TracedLogger =
      DomainTopologyTransactionMessageValidatorTest.this.logger
  }

  override def withFixture(test: OneArgTest): Outcome = {
    withFixture(test.toNoArgTest(new Env()))
  }
  protected def raiseAlarmBeyondV4(
      env: Env,
      ts: CantonTimestamp,
      msg: DomainTopologyTransactionMessage,
      assertion: (LogEntry => Assertion) = _.shouldBeCantonErrorCode(TopologyManagerAlarm),
  ): Assertion =
    loggerFactory.assertLogs(
      {
        env.inject(
          ts,
          msg,
        ) should have length (0)
      },
      assertion,
    )

  private lazy val ts1 = CantonTimestamp.Epoch
  private lazy val ts2 = CantonTimestamp.Epoch.plusMillis(100)

  "validates first self-contained message and subsequent messages" in { env =>
    import env.*

    inject(ts1, build(ts1, Txs.genesis)) should have length (4)
    parTopoClient.updateHead(
      EffectiveTime(ts1),
      ApproximateTime(ts1),
      potentialTopologyChange = true,
    )
    inject(ts2, build(ts2, List(Txs.okmMed))) should have length (1)
  }

  "skips and alerts" when {
    "observing invalid signature" in { env =>
      import env.*
      val other = build(ts2, List(Txs.okmMed))
      raiseAlarmBeyondV4(
        env,
        ts1,
        build(ts1, Txs.genesis).replaceSignatureForTesting(other.domainTopologyManagerSignature),
      )
    }
    "incomplete bootstrapping message" in { env =>
      import env.*
      raiseAlarmBeyondV4(
        env,
        ts1,
        build(ts1, List(Txs.dNs)),
        _.warningMessage should include("does not contain the participant state"),
      )
    }
    "missing signing key" in { env =>
      import env.*
      raiseAlarmBeyondV4(
        env,
        ts1,
        build(ts1, List(Txs.dNs, Txs.okmSeq, Txs.trustCert)),
        _.warningMessage should include("Domain manager signature with unknown key"),
      )
    }

    "observing invalid sequencing times" in { env =>
      import env.*
      raiseAlarmBeyondV4(
        env,
        ts2,
        build(ts1, Txs.genesis),
        _.warningMessage should include(
          "Detected malicious replay of a domain topology transaction message"
        ),
      )
    }

  }

}
