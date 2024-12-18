// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainCryptoPureApi
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.TopologyStateProcessor
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{
  DomainParametersState,
  TopologyChangeOp,
  ValidatingTopologyMappingChecks,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.concurrent.ExecutionContext

class TopologyTransactionHandling(
    pureCrypto: DomainCryptoPureApi,
    store: TopologyStore[TopologyStoreId.DomainStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  protected val timeAdjuster =
    new TopologyTimestampPlusEpsilonTracker(store, timeouts, loggerFactory)

  protected val stateProcessor = new TopologyStateProcessor(
    store,
    None,
    new ValidatingTopologyMappingChecks(store, loggerFactory),
    pureCrypto,
    loggerFactory,
  )

  protected def inspectAndAdvanceTopologyTransactionDelay(
      effectiveTimestamp: EffectiveTime,
      validated: Seq[GenericValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Unit = {
    val domainParamChanges = validated.flatMap(
      _.collectOf[TopologyChangeOp.Replace, DomainParametersState]
        .filter(tx =>
          tx.rejectionReason.isEmpty && !tx.transaction.isProposal && !tx.expireImmediately
        )
        .map(_.mapping)
    )

    domainParamChanges match {
      case Seq() => // normally, we shouldn't have any adjustment
      case Seq(domainParametersState) =>
        // Report adjustment of topologyChangeDelay
        timeAdjuster.adjustTopologyChangeDelay(
          effectiveTimestamp,
          domainParametersState.parameters.topologyChangeDelay,
        )

      case _: Seq[DomainParametersState] =>
        // As all DomainParametersState transactions have the same `uniqueKey`,
        // the topologyTransactionProcessor ensures that only the last one is committed.
        // All other DomainParameterState are rejected or expired immediately.
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Unable to commit several DomainParametersState transactions at the same effective time.\n$validated"
          )
        )
    }
  }

}
