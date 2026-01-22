// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, TopologyConfig}
import com.digitalasset.canton.crypto.{SigningKeyUsage, SynchronizerCryptoPureApi}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.store.db.DbTopologyStoreHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  SignedTopologyTransaction,
}
import com.digitalasset.canton.version.{HasTestCloseContext, ProtocolVersionValidation}
import com.digitalasset.canton.{FailOnShutdown, HasActorSystem}

abstract class InitialTopologySnapshotValidatorTest
    extends TopologyTransactionHandlingBase
    with HasActorSystem
    with FailOnShutdown
    with HasTestCloseContext {

  import Factory.*

  protected def mkDefault() = mk(
    mkStore(
      Factory.physicalSynchronizerId1a,
      "initial-validation",
    )
  )

  protected def mk(
      store: TopologyStore[TopologyStoreId.SynchronizerStore]
  ): (InitialTopologySnapshotValidator, TopologyStore[TopologyStoreId.SynchronizerStore]) = {

    val validator = new InitialTopologySnapshotValidator(
      new SynchronizerCryptoPureApi(defaultStaticSynchronizerParameters, crypto),
      store,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting.copy(validateInitialTopologySnapshot = true),
      staticSynchronizerParameters = Some(defaultStaticSynchronizerParameters),
      timeouts,
      loggerFactory,
      cleanupTopologySnapshot = true,
    )
    (validator, store)
  }

  "processing the initial topology snapshot" should {
    "successfully process the genesis state at topology initialization time" in {

      val timestampForInit = SignedTopologyTransaction.InitialTopologySequencingTime
      val genesisState = StoredTopologyTransactions(
        List(
          // transaction -> expireImmediately
          ns1k1_k1 -> false,
          dmp1_k1 -> false,
          ns2k2_k2 -> false,
          ns3k3_k3 -> false,
          ns1k2_k1 -> false,
          ns3k3_k3 -> false, // check that duplicates are properly processed
          dnd_proposal_k1 -> true,
          dnd_proposal_k2 -> true,
          okm1bk5k1E_k1 -> false,
          dtcp1_k1 -> false,
          dnd_proposal_k3
            .copy(isProposal = false)
            .addSignatures(dnd_proposal_k1.signatures)
            .addSignatures(dnd_proposal_k2.signatures)
            -> false,
        ).map { case (tx, expireImmediately) =>
          StoredTopologyTransaction(
            SequencedTime(timestampForInit),
            EffectiveTime(timestampForInit),
            validUntil = Option.when(expireImmediately)(EffectiveTime(timestampForInit)),
            tx,
            None,
          )
        }
      )
      val (validator, store) = mkDefault()

      val result = validator.validateAndApplyInitialTopologySnapshot(genesisState).futureValueUS
      result shouldBe Right(())
      val stateAfterInitialization = fetch(store, timestampForInit.immediateSuccessor)
      validate(stateAfterInitialization, genesisState.result.map(_.transaction))
    }

    "successfully process the genesis state at topology initialization time ignoring missing signatures of signing keys" in {

      val timestampForInit = SignedTopologyTransaction.InitialTopologySequencingTime
      val genesisState = StoredTopologyTransactions(
        List(
          ns1k1_k1,
          dmp1_k1,
          ns2k2_k2,
          ns3k3_k3,
          ns1k2_k1,
          okm1bk5k1E_k1,
          dtcp1_k1,
          okmS1k7_k1.removeSignatures(Set(SigningKeys.key7.fingerprint)).value,
        ).map(tx =>
          StoredTopologyTransaction(
            SequencedTime(timestampForInit),
            EffectiveTime(timestampForInit),
            validUntil = None,
            tx,
            None,
          )
        )
      )

      val (validator, store) = mkDefault()

      val result = validator.validateAndApplyInitialTopologySnapshot(genesisState).futureValueUS
      result shouldBe Right(())

      val stateAfterInitialization = fetch(store, timestampForInit.immediateSuccessor)
      validate(stateAfterInitialization, genesisState.result.map(_.transaction))
    }

    "successfully process the genesis state at topology initialization with transactions with different signatures by the same key" in {
      val timestampForInit = SignedTopologyTransaction.InitialTopologySequencingTime

      val dnd = mkAddMultiKey(
        DecentralizedNamespaceDefinition.tryCreate(
          DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns2, ns3)),
          PositiveInt.two,
          NonEmpty(Set, ns1, ns2, ns3),
        ),
        NonEmpty(Set, SigningKeys.key1, SigningKeys.key2, SigningKeys.key3),
      )
      // construct a proto with multiple signatures, just like it would be deserialized from a pre-signature-deduplication snapshot
      val dnd_duplicate_k1_sig = {
        val dndProto = dnd.toProtoV30
        val newSig = syncCryptoClient.crypto.privateCrypto
          .sign(dnd.hash.hash, SigningKeys.key1.fingerprint, SigningKeyUsage.NamespaceOnly)
          .futureValueUS
          .value
        // specifically adding the duplicate signature first in the list of signatures.
        // this causes the new transaction to have a different signature for key1 as the original `dnd`
        val protoWithDuplicateSig =
          dndProto.copy(signatures = newSig.toProtoV30 +: dndProto.signatures)

        SignedTopologyTransaction
          .fromProtoV30(
            ProtocolVersionValidation.NoValidation,
            protoWithDuplicateSig,
          )
          .value
      }

      dnd.signatures should not be dnd_duplicate_k1_sig.signatures
      dnd.signatures.map(_.authorizingLongTermKey) shouldBe dnd_duplicate_k1_sig.signatures.map(
        _.authorizingLongTermKey
      )

      val inputTransactions = List(
        ns1k1_k1 -> false, // whether to expire immediately or not
        ns2k2_k2 -> false,
        ns3k3_k3 -> false,
        dmp1_k1 -> false,
        dnd -> true,
        dnd_duplicate_k1_sig -> false,
        okm1bk5k1E_k1 -> false,
      )

      // the DNDs in the input transactions will get deduplicated and only the original DND is retained.
      // the second one is effectively the same.
      val expectedTransactions = List(
        ns1k1_k1,
        ns2k2_k2,
        ns3k3_k3,
        dmp1_k1,
        dnd, // the stored DND should not have validUntil set.
        okm1bk5k1E_k1,
      ).map(_ -> false)

      def toStored(txs: Seq[(GenericSignedTopologyTransaction, Boolean)]) =
        StoredTopologyTransactions(
          txs.map { case (tx, expireImmediately) =>
            StoredTopologyTransaction(
              SequencedTime(timestampForInit),
              EffectiveTime(timestampForInit),
              validUntil = Option.when(expireImmediately)(EffectiveTime(timestampForInit)),
              tx,
              None,
            )
          }
        )

      val genesisState = toStored(inputTransactions)
      val (validator, store) = mkDefault()

      val result = validator.validateAndApplyInitialTopologySnapshot(genesisState).futureValueUS
      result shouldBe Right(())

      val stateAfterInitialization = fetchTx(store, timestampForInit.immediateSuccessor)
      stateAfterInitialization.result should contain theSameElementsInOrderAs toStored(
        expectedTransactions
      ).result

    }

    "reject missing signatures of signing keys if the transaction is not in the genesis topology state" in {

      val timestampForInit = SignedTopologyTransaction.InitialTopologySequencingTime
      val okmS1k7_without_k7_signature =
        okmS1k7_k1.removeSignatures(Set(SigningKeys.key7.fingerprint)).value
      val genesisState = StoredTopologyTransactions(
        List(
          ns1k1_k1,
          dmp1_k1,
          ns2k2_k2,
          ns3k3_k3,
          ns1k2_k1,
          okm1bk5k1E_k1,
          dtcp1_k1,
        ).map(tx =>
          StoredTopologyTransaction(
            SequencedTime(timestampForInit),
            EffectiveTime(timestampForInit),
            validUntil = None,
            tx,
            None,
          )
        ) :+ StoredTopologyTransaction(
          SequencedTime(ts(1)),
          EffectiveTime(
            ts(1).plus((StaticSynchronizerParameters.defaultTopologyChangeDelay.unwrap))
          ),
          validUntil = None,
          okmS1k7_without_k7_signature,
          None,
        )
      )

      val (validator, store) = mkDefault()

      val result = validator.validateAndApplyInitialTopologySnapshot(genesisState).futureValueUS
      result.left.value should include regex ("(?s)Store:.*rejectionReason = 'Not fully authorized'".r)

      val stateAfterInitialization = fetch(store, ts(2))
      // the OTK is rejected and therefore is not returned when looking up valid transactions
      validate(stateAfterInitialization, genesisState.result.map(_.transaction).dropRight(1))

      // verify that the OTK was rejected with the expected reason
      store
        .findStored(ts(2), okmS1k7_without_k7_signature, includeRejected = true)
        .futureValueUS
        .value
        .rejectionReason
        .value
        .str shouldBe "Not fully authorized"
    }

    "detect inconsistencies between the snapshot and the result of processing the transactions" in {

      val timestampForInit = SignedTopologyTransaction.InitialTopologySequencingTime
      val correctTx = StoredTopologyTransaction(
        SequencedTime(timestampForInit),
        EffectiveTime(timestampForInit),
        validUntil = None,
        ns1k1_k1,
        None,
      )

      {
        // here it doesn't matter that ns1k1_k1 is actually a valid transaction,
        // but we want to test whether an inconsistency is reported
        val validatorDoesNotRejectTransaction =
          StoredTopologyTransaction(
            SequencedTime(timestampForInit),
            EffectiveTime(timestampForInit),
            validUntil = Some(EffectiveTime(timestampForInit)),
            ns2k2_k2,
            rejectionReason = Some(String300.tryCreate("some rejection reason")),
          )
        val (validator, _) = mkDefault()
        val result = validator
          .validateAndApplyInitialTopologySnapshot(
            // include a valid transaction as well
            StoredTopologyTransactions(Seq(correctTx, validatorDoesNotRejectTransaction))
          )
          .value
          .futureValueUS
        result.left.value should include(
          "Mismatch between transactions at index 1 from the initial snapshot and the topology store"
        )
      }

      {
        val validatorRejectsTransaction = StoredTopologyTransaction(
          SequencedTime(timestampForInit),
          EffectiveTime(timestampForInit),
          validUntil = None,
          // originally this transaction might have been valid,
          // but in the context of this topology snapshot it is not, because the authorization chain
          // is broken. Maybe somebody tampered with the topology snapshot after exporting it or there
          // is a bug in the export logic.
          // Regardless, we want the validator to report the inconsistency
          ns1k3_k2,
          rejectionReason = None,
        )
        val (validator, _) = mkDefault()
        val result = validator
          .validateAndApplyInitialTopologySnapshot(
            // include a valid transaction as well
            StoredTopologyTransactions(Seq(correctTx, validatorRejectsTransaction))
          )
          .value
          .futureValueUS
        result.left.value should include(
          "Mismatch between transactions at index 1 from the initial snapshot and the topology store"
        )
      }

    }
  }

}

class InitialTopologySnapshotValidatorTestInMemory extends InitialTopologySnapshotValidatorTest {
  protected def mkStore(
      synchronizerId: PhysicalSynchronizerId = Factory.physicalSynchronizerId1,
      testName: String,
  ): TopologyStore[TopologyStoreId.SynchronizerStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(synchronizerId),
      testedProtocolVersion,
      loggerFactory.appendUnnamedKey("testName", testName),
      timeouts,
    )

}
class InitialTopologySnapshotValidatorTestPostgres
    extends InitialTopologySnapshotValidatorTest
    with DbTest
    with DbTopologyStoreHelper
    with PostgresTest
