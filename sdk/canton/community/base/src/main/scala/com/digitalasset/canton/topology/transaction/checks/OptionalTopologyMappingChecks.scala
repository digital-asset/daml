// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction.checks

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.{
  TimeQuery,
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.checks.TopologyMappingChecks.PendingChangesLookup
import com.digitalasset.canton.topology.transaction.{
  MediatorSynchronizerState,
  OwnerToKeyMapping,
  PartyToParticipant,
  SequencerSynchronizerState,
  SignedTopologyTransaction,
  SynchronizerTrustCertificate,
  TopologyChangeOp,
}
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.ExecutionContext

/** Topology mapping checks preventing a user to accidentally break their node
  *
  * The following checks run as part of the topology manager write step and are there to avoid
  * breaking the topology state by accident.
  */
class OptionalTopologyMappingChecks(
    store: TopologyStore[TopologyStoreId],
    loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingChecksWithStore(MaybeEmptyTopologyStore(store), loggerFactory) {

  private def loadHistoryFromStore(
      effectiveTime: EffectiveTime,
      code: Code,
      pendingChangesLookup: PendingChangesLookup,
      maxSerialExclusive: PositiveInt,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Seq[
    GenericSignedTopologyTransaction
  ]] =
    EitherT.right[TopologyTransactionRejection](
      store
        .inspect(
          proposals = false,
          // effective time has exclusive semantics, but TimeQuery.Range.until has always had inclusive semantics.
          // therefore, we take the immediatePredecessor here
          timeQuery =
            TimeQuery.Range(from = None, until = Some(effectiveTime.value.immediatePredecessor)),
          asOfExclusiveO = None,
          op = None,
          types = Seq(code),
          idFilter = None,
          namespaceFilter = None,
        )
        .map { storedTxs =>
          val pending = pendingChangesLookup.values
            .filter(pendingTx =>
              !pendingTx.currentTx.isProposal && pendingTx.currentTx.transaction.mapping.code == code
            )
            .map(_.currentTx)
          val allTransactions = (storedTxs.result.map(_.transaction) ++ pending)
          // only look at the >history< of the mapping (up to exclusive the max serial), because
          // otherwise it would be looking also at the future, which could lead to the wrong conclusion
          // (eg detecting a member as "rejoining".
          allTransactions.filter(_.serial < maxSerialExclusive)
        }
    )

  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      pendingChangesLookup: PendingChangesLookup,
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    lazy val checkOpt = (toValidate.mapping.code, inStore.map(_.mapping.code)) match {
      case (Code.OwnerToKeyMapping, None | Some(Code.OwnerToKeyMapping)) =>
        val checkRemove = toValidate
          .select[TopologyChangeOp.Remove, OwnerToKeyMapping]
          .map(
            checkOwnerToKeyMappingRemove(
              effective,
              _,
              pendingChangesLookup,
            )
          )
        checkRemove
      case (Code.SynchronizerTrustCertificate, None | Some(Code.SynchronizerTrustCertificate)) =>
        val checkRemove = toValidate
          .select[TopologyChangeOp.Remove, SynchronizerTrustCertificate]
          .map(checkSynchronizerTrustCertificateRemove(effective, _, pendingChangesLookup))

        checkRemove

      case (Code.MediatorSynchronizerState, None | Some(Code.MediatorSynchronizerState)) =>
        toValidate
          .select[TopologyChangeOp.Replace, MediatorSynchronizerState]
          .map(
            checkMediatorSynchronizerStateReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, MediatorSynchronizerState]),
              pendingChangesLookup,
            )
          )
      case (Code.SequencerSynchronizerState, None | Some(Code.SequencerSynchronizerState)) =>
        toValidate
          .select[TopologyChangeOp.Replace, SequencerSynchronizerState]
          .map(
            checkSequencerSynchronizerStateReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, SequencerSynchronizerState]),
              pendingChangesLookup,
            )
          )
      case _otherwise => None
    }
    checkOpt.getOrElse(EitherTUtil.unitUS)
  }

  private def ensureParticipantDoesNotHostParties(
      effective: EffectiveTime,
      participantId: ParticipantId,
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext) =
    for {
      storedPartyToParticipantMappings <- loadFromStore(
        effective,
        Set(Code.PartyToParticipant),
        pendingChangesLookup.values,
      )
      participantHostsParties = storedPartyToParticipantMappings.view
        .flatMap(_.selectMapping[PartyToParticipant])
        .collect {
          case tx if tx.mapping.participants.exists(_.participantId == participantId) =>
            tx.mapping.partyId
        }
        .toSeq
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
        participantHostsParties.isEmpty,
        TopologyTransactionRejection.OptionalMapping.ParticipantStillHostsParties(
          participantId,
          participantHostsParties,
        ),
      )
    } yield ()

  private def checkOwnerToKeyMappingRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Remove, OwnerToKeyMapping],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    toValidate.mapping.member match {
      case participantId: ParticipantId =>
        // this means that we can also remove some checks from the party onboarding
        ensureParticipantDoesNotHostParties(effective, participantId, pendingChangesLookup)
      case _ => EitherTUtil.unitUS
    }

  private def checkSynchronizerTrustCertificateRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp, SynchronizerTrustCertificate],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    /* Checks that the DTC is not being removed if the participant still hosts a party.
     * This check is potentially quite expensive: we have to fetch all party to participant mappings, because
     * we cannot index by the hosting participants.
     */
    ensureParticipantDoesNotHostParties(
      effective,
      toValidate.mapping.participantId,
      pendingChangesLookup,
    )

  private def checkMediatorSynchronizerStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState],
      inStore: Option[
        SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState]
      ],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {

    val newMediators = (toValidate.mapping.allMediatorsInGroup.toSet -- inStore.toList.flatMap(
      _.mapping.allMediatorsInGroup
    )).map(identity[Member])

    def checkMediatorsDontRejoin()
        : EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      loadHistoryFromStore(
        effectiveTime,
        code = Code.MediatorSynchronizerState,
        pendingChangesLookup,
        toValidate.serial,
      )
        .flatMap { mdsHistory =>
          val allMediatorsPreviouslyOnSynchronizer = mdsHistory.view
            .flatMap(_.selectMapping[MediatorSynchronizerState])
            .flatMap(_.mapping.allMediatorsInGroup)
            .toSet[Member]
          val rejoiningMediators = newMediators.intersect(allMediatorsPreviouslyOnSynchronizer)
          EitherTUtil.condUnitET(
            rejoiningMediators.isEmpty,
            TopologyTransactionRejection.OptionalMapping.MembersCannotRejoinSynchronizer(
              rejoiningMediators.toSeq
            ),
          )
        }

    checkMediatorsDontRejoin()

  }

  private def checkSequencerSynchronizerStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState],
      inStore: Option[
        SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState]
      ],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    val newSequencers = (toValidate.mapping.allSequencers.toSet -- inStore.toList.flatMap(
      _.mapping.allSequencers
    )).map(identity[Member])

    def checkSequencersDontRejoin()
        : EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      loadHistoryFromStore(
        effectiveTime,
        code = Code.SequencerSynchronizerState,
        pendingChangesLookup,
        toValidate.serial,
      )
        .flatMap { sdsHistory =>
          val allSequencersPreviouslyOnSynchronizer = sdsHistory.view
            .flatMap(_.selectMapping[SequencerSynchronizerState])
            .flatMap(_.mapping.allSequencers)
            .toSet[Member]
          val rejoiningSequencers = newSequencers.intersect(allSequencersPreviouslyOnSynchronizer)
          EitherTUtil.condUnitET(
            rejoiningSequencers.isEmpty,
            TopologyTransactionRejection.OptionalMapping
              .MembersCannotRejoinSynchronizer(rejoiningSequencers.toSeq),
          )
        }

    checkSequencersDontRejoin()
  }

}
