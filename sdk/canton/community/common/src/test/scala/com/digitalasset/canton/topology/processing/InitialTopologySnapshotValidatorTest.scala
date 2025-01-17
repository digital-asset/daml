// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.FailOnShutdown
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.SynchronizerCryptoPureApi
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.db.DbTopologyStoreHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction

abstract class InitialTopologySnapshotValidatorTest
    extends TopologyTransactionHandlingBase
    with FailOnShutdown {

  import Factory.*

  protected def mk(
      store: TopologyStore[TopologyStoreId.SynchronizerStore] = mkStore(Factory.synchronizerId1a),
      synchronizerId: SynchronizerId = Factory.synchronizerId1a,
      insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot: Boolean = false,
  ): (InitialTopologySnapshotValidator, TopologyStore[TopologyStoreId.SynchronizerStore]) = {

    val validator = new InitialTopologySnapshotValidator(
      synchronizerId,
      testedProtocolVersion,
      new SynchronizerCryptoPureApi(defaultStaticSynchronizerParameters, crypto),
      store,
      insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot =
        insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
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
          dnd_proposal_k1 -> true,
          dnd_proposal_k2 -> true,
          dtcp1_k1 -> false,
          dnd_proposal_k3
            .copy(isProposal = false)
            .addSignatures(
              dnd_proposal_k1.signatures.toSeq ++ dnd_proposal_k2.signatures.toSeq
            )
            -> false,
          okm1bk5k1E_k1 -> false,
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
      val (validator, store) = mk()

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
          dtcp1_k1,
          okm1bk5k1E_k1,
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
      val (validator, store) = mk(insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot = true)

      val result = validator.validateAndApplyInitialTopologySnapshot(genesisState).futureValueUS
      result shouldBe Right(())
      val stateAfterInitialization = fetch(store, timestampForInit.immediateSuccessor)
      validate(stateAfterInitialization, genesisState.result.map(_.transaction))
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
        val (validator, _) = mk()
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
        val (validator, _) = mk()
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
      synchronizerId: SynchronizerId = SynchronizerId(Factory.uid1a)
  ): TopologyStore[TopologyStoreId.SynchronizerStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(synchronizerId),
      testedProtocolVersion,
      loggerFactory,
      timeouts,
    )

}
class InitialTopologySnapshotValidatorTestPostgres
    extends InitialTopologySnapshotValidatorTest
    with DbTest
    with DbTopologyStoreHelper
    with PostgresTest {
  override protected def mkStore(
      synchronizerId: SynchronizerId
  ): TopologyStore[TopologyStoreId.SynchronizerStore] =
    createTopologyStore(synchronizerId)
}
