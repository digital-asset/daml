// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction.checks

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.{
  TimeQuery,
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{
  MediatorSynchronizerState,
  SequencerSynchronizerState,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
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
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingChecks
    with NamedLogging {

  private def loadHistoryFromStore(
      effectiveTime: EffectiveTime,
      code: Code,
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
          // only look at the >history< of the mapping (up to exclusive the max serial), because
          // otherwise it would be looking also at the future, which could lead to the wrong conclusion
          // (eg detecting a member as "rejoining".
          storedTxs.result.map(_.transaction).filter(_.serial < maxSerialExclusive)
        }
    )

  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    lazy val checkOpt = (toValidate.mapping.code, inStore.map(_.mapping.code)) match {

      case (Code.MediatorSynchronizerState, None | Some(Code.MediatorSynchronizerState)) =>
        toValidate
          .select[TopologyChangeOp.Replace, MediatorSynchronizerState]
          .map(
            checkMediatorSynchronizerStateReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, MediatorSynchronizerState]),
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
            )
          )
      case _otherwise => None
    }
    checkOpt.getOrElse(EitherTUtil.unitUS)
  }

  private def checkMediatorSynchronizerStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState],
      inStore: Option[
        SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState]
      ],
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
