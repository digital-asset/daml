// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.list.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoPureApi,
  DomainSyncCryptoClient,
  PublicKey,
  SigningPublicKey,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.{
  BaseCachingDomainTopologyClient,
  CachingDomainTopologyClient,
  StoreBasedDomainTopologyClient,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Positive
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

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
    validator: DomainTopologyTransactionMessageValidator,
    pureCrypto: CryptoPureApi,
    store: TopologyStore[TopologyStoreId.DomainStore],
    acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit,
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessorCommonImpl[SignedTopologyTransaction[TopologyChangeOp]](
      domainId,
      futureSupervisor,
      store,
      acsCommitmentScheduleEffectiveTime,
      timeouts,
      loggerFactory,
    )
    with HasCloseContext {

  private val authValidator =
    new IncomingTopologyTransactionAuthorizationValidator(
      pureCrypto,
      store,
      Some(domainId),
      loggerFactory.append("role", "incoming"),
    )

  override type SubscriberType = TopologyTransactionProcessingSubscriber

  override protected def epsilonForTimestamp(
      asOfExclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologyStore.Change.TopologyDelay] =
    TopologyTimestampPlusEpsilonTracker.epsilonForTimestamp(
      store,
      asOfExclusive,
    )

  override protected def maxTimestampFromStore()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = store.timestamp(useStateStore = true)

  override protected def initializeTopologyTimestampPlusEpsilonTracker(
      processorTs: CantonTimestamp,
      maxStored: Option[SequencedTime],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[EffectiveTime] = {

    // remember the timestamp of the last message in order to validate the signatures
    // of the domain topology transaction message. we use the minimum of the two timestamps given,
    // should give us the latest state that is valid at the point of the new transaction
    // this means that if we validate the transaction, we validate it against the
    // as-of-inclusive state of the previous transaction
    val latestMsg = processorTs.min(maxStored.map(_.value).getOrElse(processorTs))
    if (latestMsg > CantonTimestamp.MinValue) {
      validator.initLastMessageTimestamp(Some(latestMsg))
    }

    TopologyTimestampPlusEpsilonTracker.initialize(timeAdjuster, store, processorTs)
  }

  @VisibleForTesting
  override private[processing] def process(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // start validation and change delay advancing
    val validatedF = performUnlessClosingF(functionFullName)(
      authValidator.validateAndUpdateHeadAuthState(effectiveTimestamp.value, transactions).map {
        case ret @ (_, validated) =>
          inspectAndAdvanceTopologyTransactionDelay(
            effectiveTimestamp,
            sequencingTimestamp,
            validated,
          )
          ret
      }
    )

    // string approx for output
    val epsilon =
      s"${effectiveTimestamp.value.toEpochMilli - sequencingTimestamp.value.toEpochMilli}"

    // store transactions once they are fully validated
    val storeF =
      validatedF.flatMap { case (_, validated) =>
        val ln = validated.length
        validated.zipWithIndex.foreach {
          case (ValidatedTopologyTransaction(tx, None), idx) =>
            logger.info(
              s"Storing topology transaction ${idx + 1}/$ln ${tx.transaction.op} ${tx.transaction.element.mapping} with ts=$effectiveTimestamp (epsilon=${epsilon} ms)"
            )
          case (ValidatedTopologyTransaction(tx, Some(r)), idx) =>
            logger.warn(
              s"Rejected transaction ${idx + 1}/$ln ${tx.transaction.op} ${tx.transaction.element.mapping} at ts=$effectiveTimestamp (epsilon=${epsilon} ms) due to $r"
            )
        }

        performUnlessClosingF(functionFullName)(
          store.append(sequencingTimestamp, effectiveTimestamp, validated)
        )
      }

    // collect incremental and full updates
    val collectedF = validatedF.map { case (cascadingUpdate, validated) =>
      (cascadingUpdate, collectIncrementalUpdate(cascadingUpdate, validated))
    }

    // incremental updates can be written asap
    val incrementalF = collectedF.flatMap { case (_, incremental) =>
      performIncrementalUpdates(sequencingTimestamp, effectiveTimestamp, incremental)
    }

    // cascading updates need to wait until the transactions have been stored
    val cascadingF = collectedF.flatMap { case (cascading, _) =>
      for {
        _ <- storeF
        _ <- performCascadingUpdates(sequencingTimestamp, effectiveTimestamp, cascading)
      } yield {}
    }

    // resynchronize
    for {
      validated <- validatedF
      (_, validatedTxs) = validated
      _ <- incrementalF
      _ <- cascadingF // does synchronize storeF
      filtered = validatedTxs.collect {
        case transaction if transaction.rejectionReason.isEmpty => transaction.transaction
      }
      _ <- listeners.toList.parTraverse(
        _.observed(
          sequencingTimestamp,
          effectiveTimestamp = effectiveTimestamp,
          sc,
          filtered,
        )
      )
    } yield ()
  }

  private def inspectAndAdvanceTopologyTransactionDelay(
      effectiveTimestamp: EffectiveTime,
      sequencingTimestamp: SequencedTime,
      validated: Seq[ValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Unit = {
    def applyEpsilon(change: DomainParametersChange): Unit = {
      timeAdjuster
        .adjustEpsilon(
          effectiveTimestamp,
          sequencingTimestamp,
          change.domainParameters.topologyChangeDelay,
        )
        .foreach { previous =>
          logger.info(
            s"Updated topology change delay from=${previous} to ${change.domainParameters.topologyChangeDelay}"
          )
        }
      timeAdjuster.effectiveTimeProcessed(effectiveTimestamp)
    }
    val domainParamChanges = validated
      .collect {
        case validatedTx
            if validatedTx.rejectionReason.isEmpty && validatedTx.transaction.transaction.op == TopologyChangeOp.Replace =>
          validatedTx.transaction.transaction.element
      }
      .collect { case DomainGovernanceElement(change: DomainParametersChange) => change }
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

  /** pick the transactions which we can process using incremental updates */
  private def collectIncrementalUpdate(
      cascadingUpdate: UpdateAggregation,
      transactions: Seq[ValidatedTopologyTransaction],
  ): Seq[SignedTopologyTransaction[TopologyChangeOp]] = {
    def isCascading(elem: SignedTopologyTransaction[TopologyChangeOp]): Boolean = {
      elem.transaction.element.mapping.requiredAuth match {
        // namespace delegation changes are always cascading
        case RequiredAuth.Ns(_, true) => true
        // identifier delegation changes are only cascading with respect to namespace
        case RequiredAuth.Ns(namespace, false) =>
          cascadingUpdate.cascadingNamespaces.contains(namespace)
        // all others are cascading if there is at least one uid affected by the cascading update
        case RequiredAuth.Uid(uids) => uids.exists(cascadingUpdate.isCascading)
      }
    }
    transactions.filter(_.rejectionReason.isEmpty).map(_.transaction).filterNot(isCascading)
  }

  private def performIncrementalUpdates(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val (deactivate, positive) = SignedTopologyTransactions(transactions).splitForStateUpdate
    performUnlessClosingF(functionFullName)(
      store.updateState(
        sequenced,
        effective,
        deactivate = deactivate,
        positive = positive,
      )
    )
  }

  private def determineUpdates(
      currents: PositiveSignedTopologyTransactions,
      targets: PositiveSignedTopologyTransactions,
  )(implicit
      traceContext: TraceContext
  ): (Seq[UniquePath], Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]]) = {

    val (toRemoveForAdds, toAddForAdds) =
      determineRemovesAdds(currents.adds.result, targets.adds.result)
    val (toRemoveForReplaces, toAddForReplaces) =
      determineRemovesAdds(currents.replaces.result, targets.replaces.result)

    (toRemoveForAdds ++ toRemoveForReplaces, toAddForAdds ++ toAddForReplaces)
  }

  private def determineRemovesAdds(
      current: Seq[SignedTopologyTransaction[Positive]],
      target: Seq[SignedTopologyTransaction[Positive]],
  )(implicit
      traceContext: TraceContext
  ) = {
    def toIndex[P <: Positive](sit: SignedTopologyTransaction[P]): (
        AuthorizedTopologyTransaction[TopologyMapping],
        SignedTopologyTransaction[P],
    ) = AuthorizedTopologyTransaction(
      sit.uniquePath,
      sit.transaction.element.mapping,
      sit,
    ) -> sit

    val currentMap = current.map(toIndex).toMap
    val targetMap = target.map(toIndex).toMap

    val currentSet = currentMap.keySet
    val targetSet = targetMap.keySet
    val toRemove = currentSet -- targetSet
    val toAdd = targetSet -- currentSet

    toRemove.foreach { item =>
      logger.debug(s"Cascading remove of $item")
    }
    toAdd.foreach { item =>
      logger.debug(s"Cascading addition of $item")
    }

    (toRemove.map(_.uniquePath).toSeq, toAdd.toSeq.flatMap(key => targetMap.get(key).toList))
  }

  private def performCascadingUpdates(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      cascadingUpdate: UpdateAggregation,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (cascadingUpdate.nothingCascading) FutureUnlessShutdown.unit
    else {
      logger.debug(
        s"Performing cascading update on namespace=${cascadingUpdate.authNamespaces} and uids=${cascadingUpdate.filteredCascadingUids}"
      )

      val uids = cascadingUpdate.filteredCascadingUids.toSeq
      val namespaces = cascadingUpdate.cascadingNamespaces.toSeq
      // filter out txs that don't fall into this namespace / uid realm, but we don't have enough
      // information on the db-level to know which tx to ignore and which one to keep
      def cascadingFilter(tx: SignedTopologyTransaction[TopologyChangeOp]): Boolean =
        tx.transaction.element.mapping.requiredAuth match {
          case RequiredAuth.Ns(namespace, _) =>
            cascadingUpdate.cascadingNamespaces.contains(namespace)
          case RequiredAuth.Uid(uids) =>
            uids.exists(uid =>
              cascadingUpdate.cascadingNamespaces.contains(uid.namespace) ||
                cascadingUpdate.filteredCascadingUids.contains(uid)
            )
        }

      for {
        target <- performUnlessClosingF(functionFullName)(
          store.findPositiveTransactions(
            asOf = effective.value,
            asOfInclusive = true,
            includeSecondary = true,
            types = DomainTopologyTransactionType.all,
            filterUid = Some(uids),
            filterNamespace = Some(namespaces),
          )
        )

        targetFiltered = target.signedTransactions.filter { tx =>
          lazy val isDomainGovernance = tx.transaction.element match {
            case _: TopologyStateUpdateElement => false
            case _: DomainGovernanceElement => true
          }

          /*
            We check that the transaction is properly authorized or is a domain governance.
            This allows not to drop domain governance transactions with cascading updates.
            In the scenario where a key authorizes a domain parameters change and is later
            revoked, the domain parameters stay valid.
           */
          val isAuthorized = authValidator.isCurrentlyAuthorized(tx) || isDomainGovernance
          cascadingFilter(tx) && isAuthorized
        }

        current <- performUnlessClosingF(functionFullName)(
          store
            .findStateTransactions(
              asOf = effective.value,
              asOfInclusive = true,
              includeSecondary = true,
              types = DomainTopologyTransactionType.all,
              filterUid = Some(uids),
              filterNamespace = Some(namespaces),
            )
        )

        currentFiltered = current.signedTransactions.filter(cascadingFilter)

        (removes, adds) = determineUpdates(currentFiltered, targetFiltered)

        _ <- performUnlessClosingF(functionFullName)(
          store.updateState(sequenced, effective, deactivate = removes, positive = adds)
        )
      } yield ()
    }

  override protected def extractTopologyUpdatesAndValidateEnvelope(
      ts: SequencedTime,
      envelopes: List[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[SignedTopologyTransaction[TopologyChangeOp]]] = {
    validator.extractTopologyUpdatesAndValidateEnvelope(ts, envelopes)
  }

  override def onClosed(): Unit = {
    super.onClosed()
    Lifecycle.close(store)(logger)
  }

}

object TopologyTransactionProcessor {

  def createProcessorAndClientForDomain(
      topologyStore: TopologyStore[TopologyStoreId.DomainStore],
      owner: Member,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      crypto: Crypto,
      initKeys: Map[Member, Seq[PublicKey]],
      parameters: CantonNodeParameters,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[
    (TopologyTransactionProcessor, BaseCachingDomainTopologyClient)
  ] = {
    val topologyClientF =
      CachingDomainTopologyClient
        .create(
          clock,
          domainId,
          protocolVersion,
          topologyStore,
          SigningPublicKey.collect(initKeys),
          StoreBasedDomainTopologyClient.NoPackageDependencies,
          parameters.cachingConfigs,
          parameters.batchingConfig,
          parameters.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
    topologyClientF.map { topologyClient =>
      val cryptoClient = new DomainSyncCryptoClient(
        owner,
        domainId,
        topologyClient,
        crypto,
        parameters.cachingConfigs,
        parameters.processingTimeouts,
        futureSupervisor,
        loggerFactory,
      )

      val topologyProcessor = new TopologyTransactionProcessor(
        domainId = domainId,
        validator = DomainTopologyTransactionMessageValidator.create(
          parameters.skipTopologyManagerSignatureValidation,
          cryptoClient,
          owner,
          protocolVersion,
          parameters.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        ),
        pureCrypto = cryptoClient.pureCrypto,
        store = topologyStore,
        acsCommitmentScheduleEffectiveTime = _ => (),
        futureSupervisor = futureSupervisor,
        timeouts = parameters.processingTimeouts,
        loggerFactory = loggerFactory,
      )
      topologyProcessor.subscribe(topologyClient)
      (topologyProcessor, topologyClient)
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
