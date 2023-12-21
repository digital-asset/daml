// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  ProtocolMessage,
  TopologyTransactionsBroadcastX,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.{
  DomainTopologyClientWithInitX,
  StoreBasedDomainTopologyClient,
  StoreBasedDomainTopologyClientX,
}
import com.digitalasset.canton.topology.store.TopologyStore.Change
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.{
  DomainParametersStateX,
  TopologyChangeOpX,
  ValidatingTopologyMappingXChecks,
}
import com.digitalasset.canton.topology.{DomainId, TopologyStateProcessorX}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class TopologyTransactionProcessorX(
    domainId: DomainId,
    crypto: Crypto,
    store: TopologyStoreX[TopologyStoreId.DomainStore],
    acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit,
    terminateProcessing: TerminateProcessing,
    enableTopologyTransactionValidation: Boolean,
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessorCommonImpl[TopologyTransactionsBroadcastX](
      domainId,
      futureSupervisor,
      store,
      acsCommitmentScheduleEffectiveTime,
      timeouts,
      loggerFactory,
    ) {

  override type SubscriberType = TopologyTransactionProcessingSubscriberX

  private val stateProcessor = new TopologyStateProcessorX(
    store,
    None,
    enableTopologyTransactionValidation,
    new ValidatingTopologyMappingXChecks(store, loggerFactory),
    crypto,
    loggerFactory,
  )

  override def onClosed(): Unit = {
    super.onClosed()
  }

  override protected def epsilonForTimestamp(asOfExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Change.TopologyDelay] =
    TopologyTimestampPlusEpsilonTracker.epsilonForTimestamp(store, asOfExclusive)

  override protected def maxTimestampFromStore()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = store.maxTimestamp()

  override protected def initializeTopologyTimestampPlusEpsilonTracker(
      processorTs: CantonTimestamp,
      maxStored: Option[SequencedTime],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[EffectiveTime] =
    TopologyTimestampPlusEpsilonTracker.initializeX(timeAdjuster, store, processorTs)

  override protected def extractTopologyUpdatesAndValidateEnvelope(
      ts: SequencedTime,
      envelopes: List[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[TopologyTransactionsBroadcastX]] = {
    FutureUnlessShutdown.pure(
      envelopes
        .mapFilter(ProtocolMessage.select[TopologyTransactionsBroadcastX])
        .map(_.protocolMessage)
    )
  }

  override private[processing] def process(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      messages: List[TopologyTransactionsBroadcastX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val tx = messages.flatMap(_.broadcasts).flatMap(_.transactions)
    performUnlessClosingEitherU("process-topology-transaction")(
      stateProcessor
        .validateAndApplyAuthorization(
          sequencingTimestamp,
          effectiveTimestamp,
          tx,
          abortIfCascading = false,
          expectFullAuthorization = false,
        )
    ).merge
      .flatMap { validated =>
        inspectAndAdvanceTopologyTransactionDelay(
          sequencingTimestamp,
          effectiveTimestamp,
          validated,
        )
        logger.debug(
          s"Notifying listeners of ${sequencingTimestamp}, ${effectiveTimestamp} and SC ${sc}"
        )
        import cats.syntax.parallel.*

        for {
          _ <- performUnlessClosingUSF("notify-topology-transaction-observers")(
            listeners.toList.parTraverse_(
              _.observed(
                sequencingTimestamp,
                effectiveTimestamp,
                sc,
                validated.collect { case tx if tx.rejectionReason.isEmpty => tx.transaction },
              )
            )
          )

          // TODO(#15089): do not notify the terminate processing for replayed events
          _ <- performUnlessClosingF("terminate-processing")(
            terminateProcessing.terminate(sc, sequencingTimestamp, effectiveTimestamp)
          )
        } yield ()

      }
  }

  private def inspectAndAdvanceTopologyTransactionDelay(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      validated: Seq[GenericValidatedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Unit = {
    def applyEpsilon(mapping: DomainParametersStateX) = {
      timeAdjuster
        .adjustEpsilon(
          effectiveTimestamp,
          sequencingTimestamp,
          mapping.parameters.topologyChangeDelay,
        )
        .foreach { previous =>
          logger.info(
            s"Updated topology change delay from=${previous} to ${mapping.parameters.topologyChangeDelay}"
          )
        }
      timeAdjuster.effectiveTimeProcessed(effectiveTimestamp)
    }

    val domainParamChanges = validated.flatMap(
      _.collectOf[TopologyChangeOpX.Replace, DomainParametersStateX]
        .filter(
          _.rejectionReason.isEmpty
        )
        .map(_.transaction.transaction.mapping)
    )

    NonEmpty.from(domainParamChanges) match {
      // normally, we shouldn't have any adjustment
      case None => timeAdjuster.effectiveTimeProcessed(effectiveTimestamp)
      case Some(changes) =>
        // if there is one, there should be exactly one
        // If we have several, let's panic now. however, we just pick the last and try to keep working
        if (changes.lengthCompare(1) > 0) {
          logger.error(
            s"Broken or malicious domain topology manager has sent (${changes.length}) domain parameter adjustments at $effectiveTimestamp, will ignore all of them except the last"
          )
        }
        applyEpsilon(changes.last1)
    }
  }

}

object TopologyTransactionProcessorX {
  def createProcessorAndClientForDomain(
      topologyStore: TopologyStoreX[TopologyStoreId.DomainStore],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      crypto: Crypto,
      parameters: CantonNodeParameters,
      enableTopologyTransactionValidation: Boolean,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): Future[(TopologyTransactionProcessorX, DomainTopologyClientWithInitX)] = {

    val processor = new TopologyTransactionProcessorX(
      domainId,
      crypto,
      topologyStore,
      _ => (),
      TerminateProcessing.NoOpTerminateTopologyProcessing,
      enableTopologyTransactionValidation,
      futureSupervisor,
      parameters.processingTimeouts,
      loggerFactory,
    )

    val client = new StoreBasedDomainTopologyClientX(
      clock,
      domainId,
      protocolVersion,
      topologyStore,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      parameters.processingTimeouts,
      futureSupervisor,
      loggerFactory,
    )

    processor.subscribe(client)
    Future.successful((processor, client))
  }
}
