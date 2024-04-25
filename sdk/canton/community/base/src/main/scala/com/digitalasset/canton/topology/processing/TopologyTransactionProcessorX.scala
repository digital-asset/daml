// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  ProtocolMessage,
  TopologyTransactionsBroadcastX,
}
import com.digitalasset.canton.sequencing.{ResubscriptionStart, SubscriptionStart}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.{
  CachingDomainTopologyClient,
  DomainTopologyClientWithInitX,
  StoreBasedDomainTopologyClient,
}
import com.digitalasset.canton.topology.store.TopologyStoreX.Change
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
import scala.math.Ordering.Implicits.*

class TopologyTransactionProcessorX(
    domainId: DomainId,
    pureCrypto: CryptoPureApi,
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

  private val maxSequencedTimeAtInitializationF =
    TraceContext.withNewTraceContext(implicit traceContext =>
      maxTimestampFromStore().map(_.map { case (sequenced, _effective) => sequenced })
    )

  private val stateProcessor = new TopologyStateProcessorX(
    store,
    None,
    enableTopologyTransactionValidation,
    new ValidatingTopologyMappingXChecks(store, loggerFactory),
    pureCrypto,
    loggerFactory,
  )

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

    // processing an event with a sequencing time less than what was already in the store
    // when initializing TopologyTransactionProcessor means that is it is being replayed
    // after crash recovery (eg reconnecting to a domain or restart after a crash)
    for {
      maxSequencedTimeAtInitialization <- performUnlessClosingF(
        "max-sequenced-time-at-initialization"
      )(
        maxSequencedTimeAtInitializationF
      )
      eventIsBeingReplayed = maxSequencedTimeAtInitialization.exists(_ >= sequencingTimestamp)

      _ = if (eventIsBeingReplayed) {
        logger.info(
          s"Replaying topology transactions at $sequencingTimestamp and SC=$sc: $tx"
        )
      }
      validated <- performUnlessClosingEitherU("process-topology-transaction")(
        stateProcessor
          .validateAndApplyAuthorization(
            sequencingTimestamp,
            effectiveTimestamp,
            tx,
            abortIfCascading = false,
            expectFullAuthorization = false,
          )
      ).merge

      _ = inspectAndAdvanceTopologyTransactionDelay(
        sequencingTimestamp,
        effectiveTimestamp,
        validated,
      )
      _ = logger.debug(
        s"Notifying listeners of ${sequencingTimestamp}, ${effectiveTimestamp} and SC ${sc}"
      )

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

      // TODO(#15089): do not notify the terminate processing for replayed events.
      //               but for some reason, this is still required, otherwise
      //               SequencerXOnboardingTombstoneTestPostgres fails
      _ <- performUnlessClosingF("terminate-processing")(
        terminateProcessing.terminate(sc, sequencingTimestamp, effectiveTimestamp)
      )
    } yield ()
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
        .filter(tx => tx.rejectionReason.isEmpty && !tx.transaction.isProposal)
        .map(_.mapping)
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
      pureCrypto: CryptoPureApi,
      parameters: CantonNodeParameters,
      enableTopologyTransactionValidation: Boolean,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[(TopologyTransactionProcessorX, DomainTopologyClientWithInitX)] = {

    val processor = new TopologyTransactionProcessorX(
      domainId,
      pureCrypto,
      topologyStore,
      _ => (),
      TerminateProcessing.NoOpTerminateTopologyProcessing,
      enableTopologyTransactionValidation,
      futureSupervisor,
      parameters.processingTimeouts,
      loggerFactory,
    )

    val cachingClientF = CachingDomainTopologyClient.createX(
      clock,
      domainId,
      protocolVersion,
      topologyStore,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      parameters.cachingConfigs,
      parameters.batchingConfig,
      parameters.processingTimeouts,
      futureSupervisor,
      loggerFactory,
    )

    cachingClientF.map { client =>
      processor.subscribe(client)
      (processor, client)
    }
  }

  /** Returns the timestamps for initializing the processor and client for a restarted or fresh subscription. */
  def subscriptionTimestamp(
      start: SubscriptionStart,
      storedTimestamps: Option[(SequencedTime, EffectiveTime)],
  ): (CantonTimestamp, Either[SequencedTime, EffectiveTime]) = {
    import SubscriptionStart.*
    start match {
      case restart: ResubscriptionStart =>
        resubscriptionTimestamp(restart)
      case FreshSubscription =>
        storedTimestamps.fold(
          // Fresh subscription with an empty domain topology store
          // processor: init at ts = min
          // client: init at ts = min
          (CantonTimestamp.MinValue, Right(EffectiveTime(CantonTimestamp.MinValue)))
        ) { case (sequenced, effective) =>
          // Fresh subscription with a bootstrapping timestamp
          // NOTE: we assume that the bootstrapping topology snapshot does not contain the first message
          // that we are going to receive from the domain
          // processor: init at max(sequence-time) of bootstrapping transactions
          // client: init at max(effective-time) of bootstrapping transactions
          (sequenced.value, Right(effective))
        }
    }
  }

  /** Returns the timestamps for initializing the processor and client for a restarted subscription. */
  def resubscriptionTimestamp(
      start: ResubscriptionStart
  ): (CantonTimestamp, Either[SequencedTime, EffectiveTime]) = {
    import SubscriptionStart.*
    start match {
      // clean-head subscription. this means that the first event we are going to get is > cleanPrehead
      // and all our stores are clean.
      // processor: initialise with ts = cleanPrehead
      // client: approximate time: cleanPrehead, knownUntil = cleanPrehead + epsilon
      //         plus, there might be "effective times" > cleanPrehead, so we need to schedule the adjustment
      //         of the approximate time to the effective time
      case CleanHeadResubscriptionStart(cleanPrehead) =>
        (cleanPrehead, Left(SequencedTime(cleanPrehead)))
      // dirty or replay subscription.
      // processor: initialise with firstReplayed.predecessor, as the next message we'll be getting is the firstReplayed
      // client: same as clean-head resubscription
      case ReplayResubscriptionStart(firstReplayed, Some(cleanPrehead)) =>
        (firstReplayed.immediatePredecessor, Left(SequencedTime(cleanPrehead)))
      // dirty re-subscription of a node that crashed before fully processing the first event
      // processor: initialise with firstReplayed.predecessor, as the next message we'll be getting is the firstReplayed
      // client: initialise client with firstReplayed (careful: firstReplayed is known, but firstReplayed.immediateSuccessor not)
      case ReplayResubscriptionStart(firstReplayed, None) =>
        (
          firstReplayed.immediatePredecessor,
          Right(EffectiveTime(firstReplayed.immediatePredecessor)),
        )
    }
  }
}
