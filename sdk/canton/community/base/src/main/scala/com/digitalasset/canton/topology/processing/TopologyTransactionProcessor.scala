// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  ProtocolMessage,
  TopologyTransactionsBroadcast,
}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.{Deliver, DeliverError}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.client.{
  CachingDomainTopologyClient,
  DomainTopologyClientWithInit,
  StoreBasedDomainTopologyClient,
}
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor.subscriptionTimestamp
import com.digitalasset.canton.topology.store.TopologyStore.Change
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{
  DomainParametersState,
  TopologyChangeOp,
  ValidatingTopologyMappingChecks,
}
import com.digitalasset.canton.topology.{DomainId, TopologyManagerError, TopologyStateProcessor}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.*

/** Main incoming topology transaction validation and processing
  *
  * The topology transaction processor is subscribed to the event stream and processes
  * the domain topology transactions sent via the sequencer.
  *
  * It validates and then computes the updates to the data store in order to be able
  * to represent the topology state at any point in time.
  *
  * The processor works together with the StoreBasedDomainTopologyClient
  */
class TopologyTransactionProcessor(
    domainId: DomainId,
    pureCrypto: CryptoPureApi,
    store: TopologyStore[TopologyStoreId.DomainStore],
    acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit,
    terminateProcessing: TerminateProcessing,
    futureSupervisor: FutureSupervisor,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val initialised = new AtomicBoolean(false)

  private val listeners = ListBuffer[TopologyTransactionProcessingSubscriber]()

  private val timeAdjuster =
    new TopologyTimestampPlusEpsilonTracker(timeouts, loggerFactory, futureSupervisor)

  private val serializer = new SimpleExecutionQueue(
    "topology-transaction-processor-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  /** assumption: subscribers don't do heavy lifting */
  final def subscribe(listener: TopologyTransactionProcessingSubscriber): Unit = {
    listeners += listener
  }

  private def initialise(
      start: SubscriptionStart,
      domainTimeTracker: DomainTimeTracker,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    ErrorUtil.requireState(
      !initialised.getAndSet(true),
      "topology processor is already initialised",
    )

    def initClientFromSequencedTs(
        sequencedTs: SequencedTime
    ): FutureUnlessShutdown[NonEmpty[Seq[(EffectiveTime, ApproximateTime)]]] = for {
      // we need to figure out any future effective time. if we had been running, there would be a clock
      // scheduled to poke the domain client at the given time in order to adjust the approximate timestamp up to the
      // effective time at the given point in time. we need to recover these as otherwise, we might be using outdated
      // topology snapshots on startup. (wouldn't be tragic as by getting the rejects, we'd be updating the timestamps
      // anyway).
      upcoming <- performUnlessClosingF(functionFullName)(
        store.findUpcomingEffectiveChanges(sequencedTs.value)
        // find effective time of sequenced Ts (directly from store)
        // merge times
      )
      currentEpsilon <- epsilonForTimestamp(sequencedTs.value)
    } yield {
      // we have (ts+e, ts) and quite a few te in the future, so we create list of upcoming changes and sort them

      val head = (
        EffectiveTime(sequencedTs.value.plus(currentEpsilon.epsilon.unwrap)),
        ApproximateTime(sequencedTs.value),
      )
      val tail = upcoming.map(x => (x.effective, x.effective.toApproximate))

      NonEmpty(Seq, head, tail*).sortBy { case (effectiveTime, _) => effectiveTime.value }
    }

    for {
      stateStoreTsO <- performUnlessClosingF(functionFullName)(
        maxTimestampFromStore()
      )
      (processorTs, clientTs) = subscriptionTimestamp(start, stateStoreTsO)
      _ <- initializeTopologyTimestampPlusEpsilonTracker(processorTs)

      clientInitTimes <- clientTs match {
        case Left(sequencedTs) =>
          // approximate time is sequencedTs
          initClientFromSequencedTs(sequencedTs)
        case Right(effective) =>
          // effective and approximate time are effective time
          FutureUnlessShutdown.pure(NonEmpty(Seq, (effective, effective.toApproximate)))
      }
    } yield {
      logger.debug(
        s"Initializing topology processing for start=$start with effective ts ${clientInitTimes.map(_._1)}"
      )

      // let our client know about the latest known information right now, but schedule the updating
      // of the approximate time subsequently
      val maxEffective = clientInitTimes.map { case (effective, _) => effective }.max1
      val minApproximate = clientInitTimes.map { case (_, approximate) => approximate }.min1
      listenersUpdateHead(maxEffective, minApproximate, potentialChanges = true)

      val directExecutionContext = DirectExecutionContext(noTracingLogger)
      clientInitTimes.foreach { case (effective, _approximate) =>
        // if the effective time is in the future, schedule a clock to update the time accordingly
        domainTimeTracker.awaitTick(effective.value) match {
          case None =>
            // The effective time is in the past. Directly advance our approximate time to the respective effective time
            listenersUpdateHead(effective, effective.toApproximate, potentialChanges = true)
          case Some(tickF) =>
            FutureUtil.doNotAwait(
              tickF.map(_ =>
                listenersUpdateHead(effective, effective.toApproximate, potentialChanges = true)
              )(directExecutionContext),
              "Notifying listeners to the topology processor's head",
            )
        }
      }
    }
  }

  private def listenersUpdateHead(
      effective: EffectiveTime,
      approximate: ApproximateTime,
      potentialChanges: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(
      s"Updating listener heads to ${effective} and ${approximate}. Potential changes: ${potentialChanges}"
    )
    listeners.toList.foreach(_.updateHead(effective, approximate, potentialChanges))
  }

  /** Inform the topology manager where the subscription starts when using [[processEnvelopes]] rather than [[createHandler]] */
  def subscriptionStartsAt(start: SubscriptionStart, domainTimeTracker: DomainTimeTracker)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = initialise(start, domainTimeTracker)

  /** process envelopes mostly asynchronously
    *
    * Here, we return a Future[Future[Unit]]. We need to ensure the outer future finishes processing
    * before we tick the record order publisher.
    */
  def processEnvelopes(
      sc: SequencerCounter,
      ts: SequencedTime,
      envelopes: Traced[List[DefaultOpenEnvelope]],
  ): HandlerResult =
    envelopes.withTraceContext { implicit traceContext => env =>
      internalProcessEnvelopes(
        sc,
        ts,
        extractTopologyUpdatesAndValidateEnvelope(env),
      )
    }

  private def internalProcessEnvelopes(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      updatesF: FutureUnlessShutdown[List[TopologyTransactionsBroadcast]],
  )(implicit traceContext: TraceContext): HandlerResult = {
    def computeEffectiveTime(
        updates: List[TopologyTransactionsBroadcast]
    ): FutureUnlessShutdown[EffectiveTime] = {
      if (updates.nonEmpty) {
        val effectiveTimeF =
          futureSupervisor.supervisedUS(s"adjust ts=$sequencedTime for update")(
            timeAdjuster.adjustTimestampForUpdate(sequencedTime)
          )

        // we need to inform the acs commitment processor about the incoming change
        effectiveTimeF.map { effectiveTime =>
          // this is safe to do here, as the acs commitment processor `publish` method will only be
          // invoked long after the outer future here has finished processing
          acsCommitmentScheduleEffectiveTime(Traced(effectiveTime))
          effectiveTime
        }
      } else {
        futureSupervisor.supervisedUS(s"adjust ts=$sequencedTime for update")(
          timeAdjuster.adjustTimestampForTick(sequencedTime)
        )
      }
    }

    for {
      updates <- updatesF
      _ <- ErrorUtil.requireStateAsyncShutdown(
        initialised.get(),
        s"Topology client for $domainId is not initialized. Cannot process sequenced event with counter ${sc} at ${sequencedTime}",
      )
      // compute effective time
      effectiveTime <- computeEffectiveTime(updates)
    } yield {
      // the rest, we'll run asynchronously, but sequential
      val scheduledF =
        serializer.executeUS(
          {
            if (updates.nonEmpty) {
              process(sequencedTime, effectiveTime, sc, updates)
            } else {
              tickleListeners(sequencedTime, effectiveTime)
            }
          },
          "processing topology transactions",
        )
      AsyncResult(scheduledF)
    }
  }

  private def tickleListeners(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    this.performUnlessClosingF(functionFullName) {
      Future {
        val approximate = ApproximateTime(sequencedTimestamp.value)
        listenersUpdateHead(effectiveTimestamp, approximate, potentialChanges = false)
      }
    }
  }

  def createHandler(domainId: DomainId): UnsignedProtocolEventHandler =
    new UnsignedProtocolEventHandler {

      override def name: String = s"topology-processor-$domainId"

      override def apply(
          tracedBatch: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
      ): HandlerResult = {
        MonadUtil.sequentialTraverseMonoid(tracedBatch.value) {
          _.withTraceContext { implicit traceContext =>
            {
              // TODO(#13883) Topology transactions must not specify a topology timestamp. Check this.
              case Deliver(sc, ts, _, _, batch, _) =>
                logger.debug(s"Processing sequenced event with counter $sc and timestamp $ts")
                val sequencedTime = SequencedTime(ts)
                val transactionsF = extractTopologyUpdatesAndValidateEnvelope(
                  ProtocolMessage.filterDomainsEnvelopes(
                    batch,
                    domainId,
                    (wrongMsgs: List[DefaultOpenEnvelope]) =>
                      TopologyManagerError.TopologyManagerAlarm
                        .Warn(
                          s"received messages with wrong domain ids: ${wrongMsgs.map(_.protocolMessage.domainId)}"
                        )
                        .report(),
                  )
                )
                internalProcessEnvelopes(sc, sequencedTime, transactionsF)
              case err: DeliverError =>
                internalProcessEnvelopes(
                  err.counter,
                  SequencedTime(err.timestamp),
                  FutureUnlessShutdown.pure(Nil),
                )
            }
          }
        }
      }

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          domainTimeTracker: DomainTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        TopologyTransactionProcessor.this.subscriptionStartsAt(start, domainTimeTracker)
    }

  override def onClosed(): Unit = {
    Lifecycle.close(
      timeAdjuster,
      serializer,
    )(logger)
  }

  private val maxSequencedTimeAtInitializationF =
    TraceContext.withNewTraceContext(implicit traceContext =>
      maxTimestampFromStore().map(_.map { case (sequenced, _effective) => sequenced })
    )

  private val stateProcessor = new TopologyStateProcessor(
    store,
    None,
    new ValidatingTopologyMappingChecks(store, loggerFactory),
    pureCrypto,
    loggerFactory,
  )

  private def epsilonForTimestamp(asOfExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Change.TopologyDelay] =
    TopologyTimestampPlusEpsilonTracker.epsilonForTimestamp(store, asOfExclusive)

  private def maxTimestampFromStore()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = store.maxTimestamp()

  private def initializeTopologyTimestampPlusEpsilonTracker(
      processorTs: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[EffectiveTime] =
    TopologyTimestampPlusEpsilonTracker.initialize(timeAdjuster, store, processorTs)

  private def extractTopologyUpdatesAndValidateEnvelope(
      envelopes: List[DefaultOpenEnvelope]
  ): FutureUnlessShutdown[List[TopologyTransactionsBroadcast]] = {
    FutureUnlessShutdown.pure(
      envelopes
        .mapFilter(ProtocolMessage.select[TopologyTransactionsBroadcast])
        .map(_.protocolMessage)
    )
  }

  private[processing] def process(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      messages: List[TopologyTransactionsBroadcast],
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
      //               SequencerOnboardingTombstoneTestPostgres fails
      _ <- performUnlessClosingF("terminate-processing")(
        terminateProcessing.terminate(sc, sequencingTimestamp, effectiveTimestamp)
      )
    } yield ()
  }

  private def inspectAndAdvanceTopologyTransactionDelay(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      validated: Seq[GenericValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Unit = {
    def applyEpsilon(mapping: DomainParametersState) = {
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
      _.collectOf[TopologyChangeOp.Replace, DomainParametersState]
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

object TopologyTransactionProcessor {
  abstract class Factory {
    def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit executionContext: ExecutionContext): TopologyTransactionProcessor
  }

  def createProcessorAndClientForDomain(
      topologyStore: TopologyStore[TopologyStoreId.DomainStore],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      pureCrypto: CryptoPureApi,
      parameters: CantonNodeParameters,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[(TopologyTransactionProcessor, DomainTopologyClientWithInit)] = {

    val processor = new TopologyTransactionProcessor(
      domainId,
      pureCrypto,
      topologyStore,
      _ => (),
      TerminateProcessing.NoOpTerminateTopologyProcessing,
      futureSupervisor,
      parameters.processingTimeouts,
      loggerFactory,
    )

    val cachingClientF = CachingDomainTopologyClient.create(
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
