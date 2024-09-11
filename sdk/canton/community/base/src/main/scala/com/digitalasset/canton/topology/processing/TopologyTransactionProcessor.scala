// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainCryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  ProtocolMessage,
  TopologyTransactionsBroadcast,
}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfDomain, Deliver, DeliverError}
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
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  DomainParametersState,
  TopologyChangeOp,
  ValidatingTopologyMappingChecks,
}
import com.digitalasset.canton.topology.{DomainId, TopologyManagerError, TopologyStateProcessor}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
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
    pureCrypto: DomainCryptoPureApi,
    store: TopologyStore[TopologyStoreId.DomainStore],
    acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit,
    terminateProcessing: TerminateProcessing,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val initialised = new AtomicBoolean(false)

  // Outer list designate listener groups, different groups are to be executed sequentially,
  // priority is determined by the `TopologyTransactionProcessingSubscriber.executionOrder`.
  // Inner list (subscribers with the same priority) can be executed in parallel.
  // Code calling the listeners is assuming that the structure is kept as described above,
  // with no further manipulations needed.
  private val listeners = new AtomicReference(
    List[NonEmpty[List[TopologyTransactionProcessingSubscriber]]]()
  )

  private val timeAdjuster =
    new TopologyTimestampPlusEpsilonTracker(store, timeouts, loggerFactory)

  private val serializer = new SimpleExecutionQueue(
    "topology-transaction-processor-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  /** assumption: subscribers don't do heavy lifting */
  final def subscribe(listener: TopologyTransactionProcessingSubscriber): Unit =
    listeners
      .getAndUpdate(oldListeners =>
        // we add the new listener to the pile, and then re-sort the list into groups by execution order
        (oldListeners.flatten :+ listener).distinct // .distinct guards against double subscription
          .groupBy1(_.executionOrder)
          .toList
          .sortBy { case (order, _) => order }
          .map { case (_, groupListeners) => groupListeners }
      )
      .discard

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
        EffectiveTime(sequencedTs.value.plus(currentEpsilon.changeDelay.unwrap)),
        ApproximateTime(sequencedTs.value),
      )
      val tail = upcoming.map(x => (x.validFrom, x.validFrom.toApproximate))

      NonEmpty(Seq, head, tail*).sortBy { case (effectiveTime, _) => effectiveTime.value }
    }

    for {
      stateStoreTsO <- performUnlessClosingF(functionFullName)(
        maxTimestampFromStore()
      )
      clientTs = subscriptionTimestamp(
        start,
        stateStoreTsO.map { case (_, effective) => effective },
      )

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
      s"Updating listener heads to $effective and $approximate. Potential changes: $potentialChanges"
    )
    listeners.get().flatten.foreach(_.updateHead(effective, approximate, potentialChanges))
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
      topologyTimestampO: Option[CantonTimestamp],
      envelopes: Traced[List[DefaultOpenEnvelope]],
  ): HandlerResult =
    envelopes.withTraceContext { implicit traceContext => env =>
      val broadcasts = validateEnvelopes(sc, ts, topologyTimestampO, env)
      internalProcessEnvelopes(sc, ts, broadcasts)
    }

  private def internalProcessEnvelopes(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      updates: List[TopologyTransactionsBroadcast],
  )(implicit traceContext: TraceContext): HandlerResult =
    for {
      _ <- ErrorUtil.requireStateAsyncShutdown(
        initialised.get(),
        s"Topology client for $domainId is not initialized. Cannot process sequenced event with counter $sc at $sequencedTime",
      )
    } yield {
      val txs = updates.flatMap(_.broadcasts).flatMap(_.transactions)

      // the rest, we'll run asynchronously, but sequential
      val scheduledF =
        serializer.executeUS(
          {
            val hasTransactions = txs.nonEmpty
            for {
              effectiveTime <-
                timeAdjuster.trackAndComputeEffectiveTime(sequencedTime, hasTransactions)
              _ <-
                if (hasTransactions) {
                  // we need to inform the acs commitment processor about the incoming change
                  // this is safe to do here, as the acs commitment processor `publish` method will only be
                  // invoked long after the outer future here has finished processing
                  acsCommitmentScheduleEffectiveTime(Traced(effectiveTime))

                  process(sequencedTime, effectiveTime, sc, txs)
                } else {
                  tickleListeners(sequencedTime, effectiveTime)
                }
            } yield ()
          },
          "processing topology transactions",
        )
      AsyncResult(scheduledF)
    }

  private def tickleListeners(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    this.performUnlessClosingF(functionFullName) {
      Future {
        val approximate = ApproximateTime(sequencedTimestamp.value)
        listenersUpdateHead(effectiveTimestamp, approximate, potentialChanges = false)
      }
    }

  def createHandler(domainId: DomainId): UnsignedProtocolEventHandler =
    new UnsignedProtocolEventHandler {

      override def name: String = s"topology-processor-$domainId"

      override def apply(
          tracedBatch: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
      ): HandlerResult =
        MonadUtil.sequentialTraverseMonoid(tracedBatch.value) {
          _.withTraceContext { implicit traceContext =>
            {
              case Deliver(sc, ts, _, _, batch, topologyTimestampO, _) =>
                logger.debug(s"Processing sequenced event with counter $sc and timestamp $ts")
                val sequencedTime = SequencedTime(ts)
                val envelopesForRightDomain = ProtocolMessage.filterDomainsEnvelopes(
                  batch,
                  domainId,
                  (wrongMsgs: List[DefaultOpenEnvelope]) =>
                    TopologyManagerError.TopologyManagerAlarm
                      .Warn(
                        s"received messages with wrong domain ids: ${wrongMsgs.map(_.protocolMessage.domainId)}"
                      )
                      .report(),
                )
                val broadcasts =
                  validateEnvelopes(sc, sequencedTime, topologyTimestampO, envelopesForRightDomain)
                internalProcessEnvelopes(sc, sequencedTime, broadcasts)
              case err: DeliverError =>
                internalProcessEnvelopes(
                  err.counter,
                  SequencedTime(err.timestamp),
                  Nil,
                )
            }
          }
        }

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          domainTimeTracker: DomainTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        TopologyTransactionProcessor.this.subscriptionStartsAt(start, domainTimeTracker)
    }

  /** Checks that topology broadcast envelopes satisfy the following conditions:
    * <ol>
    *   <li>the only recipient is AllMembersOfDomain</li>
    *   <li>the topology timestamp is not specified</li>
    * </ol>
    *  If any of the conditions are violated, a topology manager warning is logged and the corresponding envelope is skipped.
    *  @return the topology broadcasts that satisfy the validation conditions
    */
  private def validateEnvelopes(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      topologyTimestampO: Option[CantonTimestamp],
      envelopes: List[DefaultOpenEnvelope],
  )(implicit errorLoggingContext: ErrorLoggingContext): List[TopologyTransactionsBroadcast] = {
    val (invalidRecipients, topologyBroadcasts) = extractTopologyUpdatesWithValidRecipients(
      envelopes
    )
    if (invalidRecipients.nonEmpty) {
      TopologyManagerError.TopologyManagerAlarm
        .Warn(
          s"Discarding a topology broadcast with sc=$sc at $sequencedTime with invalid recipients: $invalidRecipients"
        )
        .report()
    }
    topologyTimestampO.filter(_ => topologyBroadcasts.nonEmpty) match {
      case Some(topologyTimestamp) =>
        // Skip processing broadcasts with an explicit topology timestamp, because:
        // 1. this could cause the group resolution to be done with the wrong timestamp
        // 2. which could lead to not all members active at sequenced time to receive the topology broadcast
        // 3. which then results in a ledger fork
        TopologyManagerError.TopologyManagerAlarm
          .Warn(
            s"Discarding a topology broadcast with sc=$sc at $sequencedTime with explicit topology timestamp $topologyTimestamp"
          )
          .report()

        // we return the empty list, to signify that we filtered out all invalid envelopes
        Nil
      case None =>
        topologyBroadcasts
    }

  }

  override def onClosed(): Unit =
    Lifecycle.close(serializer)(logger)

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
    FutureUnlessShutdown.outcomeF(store.currentChangeDelay(asOfExclusive))

  private def maxTimestampFromStore()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] =
    store.maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true)

  /** @return A tuple with list of envelopes with invalid recipients and a list of topology broadcasts to further process */
  private def extractTopologyUpdatesWithValidRecipients(
      envelopes: List[DefaultOpenEnvelope]
  ): (List[DefaultOpenEnvelope], List[TopologyTransactionsBroadcast]) =
    envelopes
      .mapFilter(ProtocolMessage.select[TopologyTransactionsBroadcast])
      .partitionMap { env =>
        Either.cond(
          // it's important that we only check that AllMembersOfDomain is existent and not the only recipient.
          // Otherwise an attacker could add a node as bcc recipient, which only that node would see and subsequently
          // discard the topology transaction, while all other nodes would happily process it and therefore lead to a ledger fork.
          env.recipients.allRecipients.contains(AllMembersOfDomain),
          env.protocolMessage,
          env,
        )
      }

  private[processing] def process(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      txs: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
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
          s"Replaying topology transactions at $sequencingTimestamp and SC=$sc: $txs"
        )
      }
      validated <- performUnlessClosingF("process-topology-transaction")(
        stateProcessor
          .validateAndApplyAuthorization(
            sequencingTimestamp,
            effectiveTimestamp,
            txs,
            expectFullAuthorization = false,
          )
      )

      _ = inspectAndAdvanceTopologyTransactionDelay(
        effectiveTimestamp,
        validated,
      )

      validTransactions = validated.collect {
        case tx if tx.rejectionReason.isEmpty && !tx.transaction.isProposal => tx.transaction
      }
      _ <- performUnlessClosingUSF("notify-topology-transaction-observers")(
        MonadUtil.sequentialTraverse_(listeners.get()) { listenerGroup =>
          logger.debug(
            s"Notifying listener group (${listenerGroup.head1.executionOrder}) of $sequencingTimestamp, $effectiveTimestamp and SC $sc"
          )
          listenerGroup.forgetNE.parTraverse_(
            _.observed(
              sequencingTimestamp,
              effectiveTimestamp,
              sc,
              validTransactions,
            )
          )
        }
      )

      // TODO(#15089): do not notify the terminate processing for replayed events.
      //               but for some reason, this is still required, otherwise
      //               SequencerOnboardingTombstoneTestPostgres fails
      _ <- performUnlessClosingF("terminate-processing")(
        terminateProcessing.terminate(sc, sequencingTimestamp, effectiveTimestamp)
      )
    } yield ()

  private def inspectAndAdvanceTopologyTransactionDelay(
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
      pureCrypto: DomainCryptoPureApi,
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
      exitOnFatalFailures = parameters.exitOnFatalFailures,
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

  /** Returns the timestamps for initializing the client for a restarted or fresh subscription. */
  def subscriptionTimestamp(
      start: SubscriptionStart,
      maxStoredEffectiveTimeO: Option[EffectiveTime],
  ): Either[SequencedTime, EffectiveTime] = {
    import SubscriptionStart.*
    start match {
      case restart: ResubscriptionStart =>
        resubscriptionTimestamp(restart)
      case FreshSubscription =>
        maxStoredEffectiveTimeO.fold(
          // Fresh subscription with an empty domain topology store
          // client: init at ts = min
          Right(EffectiveTime(CantonTimestamp.MinValue))
        ) { effective =>
          // Fresh subscription with a bootstrapping timestamp
          // NOTE: we assume that the bootstrapping topology snapshot does not contain the first message
          // that we are going to receive from the domain
          // client: init at max(effective-time) of bootstrapping transactions
          Right(effective)
        }
    }
  }

  /** Returns the timestamps for initializing the client for a restarted subscription. */
  def resubscriptionTimestamp(
      start: ResubscriptionStart
  ): Either[SequencedTime, EffectiveTime] = {
    import SubscriptionStart.*
    start match {
      // clean-head subscription. this means that the first event we are going to get is > cleanPrehead
      // and all our stores are clean.
      // client: approximate time: cleanPrehead, knownUntil = cleanPrehead + epsilon
      //         plus, there might be "effective times" > cleanPrehead, so we need to schedule the adjustment
      //         of the approximate time to the effective time
      case CleanHeadResubscriptionStart(cleanPrehead) =>
        Left(SequencedTime(cleanPrehead))
      // dirty or replay subscription.
      // client: same as clean-head resubscription
      case ReplayResubscriptionStart(_, Some(cleanPrehead)) =>
        Left(SequencedTime(cleanPrehead))
      // dirty re-subscription of a node that crashed before fully processing the first event
      // client: initialise client with firstReplayed (careful: firstReplayed is known, but firstReplayed.immediateSuccessor not)
      case ReplayResubscriptionStart(firstReplayed, None) =>
        Right(EffectiveTime(firstReplayed.immediatePredecessor))
    }
  }
}
