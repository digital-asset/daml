// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.initialization.TopologyManagementInitialization
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyDispatchingErrorGroup
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  CloseableHealthComponent,
  ComponentHealthState,
}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.protocol.messages.DomainTopologyTransactionMessage
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.{
  RequestInvalid,
  RequestRefused,
}
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  DomainTopologyClientWithInit,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{BatchTracing, TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, SequencerCounter, checked}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.math.Ordered.orderingToOrdered
import scala.util.{Failure, Success}

/** Class that dispatches topology transactions added to the domain topology manager to the members
  *
  * When topology transactions are added to a domain, the dispatcher sends them
  * to the respective members. Same is true if a participant joins a domain, the
  * dispatcher will send him the current snapshot of topology transactions.
  *
  * The dispatcher is very simple:
  * - [source] as the source topology transaction store of the domain topology manager
  * - [target] as the target topology transaction store of the domain client
  * - [queue] as the difference of the stores [source] - [target]
  *
  * Now, the dispatcher takes x :: rest from [queue], sends x over the sequencer and monitors the [target] list.
  * Once x is observed, we repeat.
  *
  * The only special thing in here is the case when x = ParticipantState update. Because in this case, a participant
  * might change its state (i.e. become online for the first time or resume operation after being disabled).
  *
  * In this case, before x is sent from [source] to [target], we send all transactions in [target] to him. And once we
  * did that, we follow-up with x.
  *
  * Given that we want that all members have the same [target] store, we send the x not to one member, but to all addressable
  * members in a batch.
  *
  * One final remark: the present code will properly resume from a crash, except that it might send a snapshot to a participant
  * a second time. I.e. it assumes idempotency of the topology transaction message processing.
  */
private[domain] class DomainTopologyDispatcher(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    processor: TopologyTransactionProcessor,
    initialKeys: Map[KeyOwner, Seq[PublicKey]],
    targetStore: TopologyStore[TopologyStoreId.DomainStore],
    crypto: Crypto,
    clock: Clock,
    addressSequencerAsDomainMember: Boolean,
    parameters: CantonNodeParameters,
    futureSupervisor: FutureSupervisor,
    val sender: DomainTopologySender,
    protected val loggerFactory: NamedLoggerFactory,
    topologyManagerSequencerCounterTrackerStore: SequencerCounterTrackerStore,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with DomainIdentityStateObserver {

  override protected val timeouts: ProcessingTimeout = parameters.processingTimeouts

  def queueSize: Int = blocking { lock.synchronized { queue.size + inflight.get() } }

  private val topologyClient =
    new StoreBasedDomainTopologyClient(
      clock,
      domainId,
      protocolVersion,
      authorizedStore,
      SigningPublicKey.collect(initialKeys),
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      timeouts,
      futureSupervisor,
      loggerFactory,
      useStateTxs = false,
    )
  private val domainSyncCryptoClient = new DomainSyncCryptoClient(
    DomainTopologyManagerId(domainId.unwrap),
    domainId,
    topologyClient,
    crypto,
    parameters.cachingConfigs,
    timeouts,
    futureSupervisor,
    loggerFactory,
  )
  private val staticDomainMembers = DomainMember.list(domainId, addressSequencerAsDomainMember)
  private val flushing = new AtomicBoolean(false)
  private val initialized = new AtomicBoolean(false)
  private val queue = mutable.Queue[Traced[StoredTopologyTransaction[TopologyChangeOp]]]()
  private val lock = new Object()
  private val catchup = new MemberTopologyCatchup(authorizedStore, loggerFactory)
  private val lastTs = new AtomicReference(CantonTimestamp.MinValue)
  private val inflight = new AtomicInteger(0)

  private sealed trait DispatchStrategy extends Product with Serializable
  private case object Alone extends DispatchStrategy
  private case object Batched extends DispatchStrategy
  private case object Last extends DispatchStrategy

  private def dispatchStrategy(mapping: TopologyMapping): DispatchStrategy = mapping match {
    // participant state changes, domain parameters changes and mediator domain states need
    // to be sent separately
    case _: ParticipantState | _: DomainParametersChange => Last
    case _: MediatorDomainState => Alone
    case _: IdentifierDelegation | _: NamespaceDelegation | _: OwnerToKeyMapping |
        _: PartyToParticipant | _: SignedLegalIdentityClaim | _: VettedPackages =>
      Batched
  }

  def init(
      flushSequencer: => FutureUnlessShutdown[Unit]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    ErrorUtil.requireState(!initialized.get(), "dispatcher has already been initialized")
    for {
      initiallyPending <- FutureUnlessShutdown.outcomeF(determinePendingTransactions())
      // synchronise topology processing if necessary to avoid duplicate submissions and ugly warnings
      actuallyPending <-
        if (initiallyPending.nonEmpty) {
          logger.debug(
            s"It seems that ${initiallyPending.size} topology transactions are still pending. Attempting to flush the sequencer before recomputing."
          )
          for {
            // flush sequencer (so we are sure that there is nothing pending)
            _ <- flushSequencer
            // now, go back to the db and re-fetch the transactions
            pending <- FutureUnlessShutdown.outcomeF(determinePendingTransactions())
          } yield pending
        } else FutureUnlessShutdown.pure(initiallyPending)
    } yield {
      actuallyPending.lastOption match {
        case Some(ts) =>
          val racy = blocking {
            lock.synchronized {
              // the domain manager might have pushed transactions already into the queue.
              // some of these transactions might have been picked up when we looked at the store.
              // we resolve the situation by trivially deduplicating
              val racy = queue
                .removeAll()
                .filterNot(racyItem => actuallyPending.exists(_.value == racyItem.value))
              queue.appendAll(actuallyPending)
              queue.appendAll(racy)
              racy
            }
          }
          updateTopologyClientTs(ts.value.validFrom.value)
          val all = actuallyPending ++ racy
          logger.info(
            show"Resuming topology dispatching with ${all.length} transactions: ${all
                .map(x => (x.value.transaction.operation, x.value.transaction.transaction.element.mapping))}"
          )
        case None =>
          logger.debug("Started domain topology dispatching (nothing to catch up)")
      }
      initialized.set(true)
      flush()
    }
  }

  override def addedSignedTopologyTransaction(
      timestamp: CantonTimestamp,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): Unit = performUnlessClosing(functionFullName) {
    val last = lastTs.getAndSet(timestamp)
    // we assume that we get txs in strict ascending timestamp order
    ErrorUtil.requireArgument(
      last < timestamp,
      s"received new topology txs with ts $timestamp which is not strictly higher than last $last",
    )
    // we assume that there is only one participant state or domain parameters change per timestamp
    // otherwise, we need to make the dispatching stuff a bit smarter and deal with "multiple participants could become active in this batch"
    // and we also can't deal with multiple replaces with the same timestamp
    val byStrategy = transactions.map(_.transaction.element.mapping).groupBy(dispatchStrategy).map {
      case (k, v) => (k, v.length)
    }
    val alone = byStrategy.getOrElse(Alone, 0)
    val batched = byStrategy.getOrElse(Batched, 0)
    val lst = byStrategy.getOrElse(Last, 0)
    ErrorUtil.requireArgument(
      (lst < 2 && alone == 0) || (alone < 2 && lst == 0 && batched == 0),
      s"received batch of topology transactions with multiple changes $byStrategy that can't be batched ${transactions}",
    )
    updateTopologyClientTs(timestamp)
    blocking {
      lock.synchronized {
        queue ++= transactions.iterator.map(x =>
          Traced(
            StoredTopologyTransaction(SequencedTime(timestamp), EffectiveTime(timestamp), None, x)
          )
        )
        flush()
      }
    }
  }.discard

  // subscribe to target
  processor.subscribe(new TopologyTransactionProcessingSubscriber {
    override def observed(
        sequencedTimestamp: SequencedTime,
        effectiveTimestamp: EffectiveTime,
        sc: SequencerCounter,
        transactions: Seq[GenericSignedTopologyTransaction],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
      transactions.foreach { tx =>
        logger.debug(
          show"Sequenced topology transaction ${tx.transaction.op} ${tx.transaction.element.mapping}"
        )
      }
      // remove the observed txs from our queue stats. as we might have some tx in flight on startup,
      // we'll just ensure we never go below 0 which will mean that once the system has resumed operation
      // the queue stats are correct, while just on startup, they might be a bit too low
      inflight.updateAndGet(x => Math.max(0, x - transactions.size)).discard
    }
  })

  private def updateTopologyClientTs(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit =
    topologyClient.updateHead(
      EffectiveTime(timestamp),
      ApproximateTime(timestamp),
      potentialTopologyChange = true,
    )

  private def determinePendingTransactions()(implicit
      traceContext: TraceContext
  ): Future[List[Traced[StoredTopologyTransaction[TopologyChangeOp]]]] =
    for {
      // fetch watermark of the domain
      watermarkTs <- targetStore.currentDispatchingWatermark
      // fetch any transaction that is potentially not dispatched
      transactions <- authorizedStore.findDispatchingTransactionsAfter(
        watermarkTs.getOrElse(CantonTimestamp.MinValue)
      )
      // filter out anything that might have ended up somehow in the target store
      // despite us not having succeeded updating the watermark
      filteredTx <- transactions.result.toList
        .parTraverseFilter { tx =>
          targetStore
            .exists(tx.transaction)
            .map(exists => Option.when(!exists)(tx))
        }
    } yield filteredTx.map(Traced(_))

  /** Determines the next batch of topology transactions to send.
    *
    * We send the entire queue up and including of the first `ParticipantState`, `MediatorDomainState` or `DomainParametersChange` update at once for efficiency reasons.
    */
  private def determineBatchFromQueue()
      : Seq[Traced[StoredTopologyTransaction[TopologyChangeOp]]] = {
    val builder = mutable.ListBuffer.newBuilder[Traced[StoredTopologyTransaction[TopologyChangeOp]]]
    @tailrec
    def go(size: Int): Unit = {
      queue.headOption.map { tx =>
        dispatchStrategy(tx.value.transaction.transaction.element.mapping)
      } match {
        // if batched, keep on batching
        case Some(Batched) =>
          builder.addOne(queue.dequeue())
          if (size < sender.maxBatchSize) go(size + 1)
        // if last, abort batching
        case Some(Last) =>
          builder.addOne(queue.dequeue())
        // if alone, only add if it is alone, else terminate
        case Some(Alone) if size == 0 => builder.addOne(queue.dequeue())
        case _ => // done
      }
    }
    go(0)
    builder.result().toSeq
  }

  /** wait an epsilon if the effective time is reduced with this change */
  private def waitIfEffectiveTimeIsReduced(
      transactions: Traced[NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]]]
  ): EitherT[FutureUnlessShutdown, String, Unit] = transactions.withTraceContext {
    implicit traceContext => txs =>
      val empty = EitherT.rightT[FutureUnlessShutdown, String](())
      txs.last1.transaction.transaction.element.mapping match {
        case mapping: DomainParametersChange =>
          EitherT
            .right(
              performUnlessClosingF(functionFullName)(
                authorizedStoreSnapshot(txs.last1.validFrom.value).findDynamicDomainParameters()
              )
            )
            .flatMap(
              _.fold(
                _ => empty,
                param => {
                  // if new epsilon is smaller than current, then wait current epsilon before dispatching this set of txs
                  val old = param.topologyChangeDelay.duration
                  if (old > mapping.domainParameters.topologyChangeDelay.duration) {
                    logger.debug(
                      s"Waiting $old due to topology change delay before resuming dispatching"
                    )
                    EitherT.right(
                      clock.scheduleAfter(_ => (), param.topologyChangeDelay.duration)
                    )
                  } else empty
                },
              )
            )

        case _ => empty
      }
  }

  private def waitIfTopologyManagerKeyWasRolled(
      transactions: NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val owner = domainId.keyOwner
    // we need to wait for the new key to become effective, as the next domain topology
    // transaction will be signed with that key ...
    val mustWait = transactions
      .map(x => (x.transaction.transaction.op, x.transaction.transaction.element.mapping))
      .exists {
        case (TopologyChangeOp.Add, OwnerToKeyMapping(`owner`, _)) => true
        case _ => false
      }
    if (!mustWait) EitherT.pure(())
    else {
      for {
        param <- EitherT
          .right(
            authorizedStoreSnapshot(transactions.head1.validFrom.value)
              .findDynamicDomainParameters()
              .map(_.toOption)
          )
          .mapK(FutureUnlessShutdown.outcomeK)
        delay = param.map(_.topologyChangeDelay).getOrElse(NonNegativeFiniteDuration.Zero)
        _ = logger.debug(
          s"Waiting $delay due to domain manager key rolling before resuming dispatching"
        )
        _ <- EitherT.right(
          clock.scheduleAfter(_ => (), delay.duration)
        )
      } yield {
        logger.debug(
          s"Resuming dispatching after key roll"
        )
      }
    }
  }

  def flush()(implicit traceContext: TraceContext): Unit = {
    if (initialized.get() && queue.nonEmpty && !flushing.getAndSet(true)) {
      val ret = for {
        pending <- EitherT.right(performUnlessClosingF(functionFullName)(Future {
          blocking {
            lock.synchronized {
              val tmp = determineBatchFromQueue()
              inflight.updateAndGet(_ + tmp.size).discard
              tmp
            }
          }
        }))
        tracedTxO = NonEmpty.from(pending).map { tracedNE =>
          BatchTracing.withTracedBatch(logger, tracedNE)(implicit traceContext =>
            txs => Traced(txs.map(_.value))
          )
        }
        _ <- tracedTxO.fold(EitherT.pure[FutureUnlessShutdown, String](()))(txs =>
          for {
            _ <- bootstrapAndDispatch(txs)
            _ <- waitIfEffectiveTimeIsReduced(txs)
          } yield ()
        )
      } yield {
        val flushAgain = blocking {
          lock.synchronized {
            flushing.getAndSet(false)
            queue.nonEmpty
          }
        }
        if (flushAgain)
          flush()
      }
      EitherTUtil.doNotAwait(
        EitherT(ret.value.onShutdown(Right(()))),
        "Halting topology dispatching due to unexpected error. Please restart.",
      )
    }

  }

  private def safeTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): CantonTimestamp = {
    if (timestamp > topologyClient.topologyKnownUntilTimestamp) {
      logger.error(
        s"Requesting invalid authorized topology timestamp at ${timestamp} while I only have ${topologyClient.topologyKnownUntilTimestamp}"
      )
      topologyClient.topologyKnownUntilTimestamp
    } else timestamp
  }

  private def authorizedCryptoSnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): DomainSnapshotSyncCryptoApi = {
    checked(domainSyncCryptoClient.trySnapshot(safeTimestamp(timestamp)))
  }

  private def authorizedStoreSnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): StoreBasedTopologySnapshot = {
    checked(topologyClient.trySnapshot(safeTimestamp(timestamp)))
  }

  /** re-order and split into three buckets to ensure that the participant gets
    * the right keys and certificates first, such that it can then subsequently
    * validate the signature on the domain topology manager message
    */
  private def splitCatchupTransactionsForParticipant(
      participant: ParticipantId,
      txs: NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]],
  ): (
      Seq[GenericSignedTopologyTransaction],
      Seq[GenericSignedTopologyTransaction],
      Seq[GenericSignedTopologyTransaction],
  ) = {
    // in order to allow participants validating the topology transaction message
    // with the right signature, we rearrange the batch such that we get:
    // - add namespace delegation, add-tx of owner to key mapping of domain and sequencer, domain trust certificate in a first batch
    // then we forward the rest of the tx. any key rolls for domain members are moved to the back
    def empty() = Seq.empty[GenericSignedTopologyTransaction]
    def special(mapping: TopologyMapping): Boolean = mapping match {
      // include domain keys
      case OwnerToKeyMapping(owner, _) => owner.uid == domainId.uid
      // include certs
      case NamespaceDelegation(namespace, _, _) =>
        namespace == domainId.uid.namespace || namespace == participant.uid.namespace
      case IdentifierDelegation(uid, _) => uid == domainId.uid || uid == participant.uid
      case _ => false
    }
    txs
      .map(x =>
        (x.transaction, x.transaction.transaction.op, x.transaction.transaction.element.mapping)
      )
      // the tuple represents the three buckets (head, mid, tail)
      .foldLeft((empty(), empty(), empty())) {
        // when sending the catchup, we need to use the last key the participant knows.
        // so we move a possible key removal of domain members to the end.
        // as the txs still need to be authorized, we also move the cert removals to the end
        case (
              (head, mid, tail),
              (tx, TopologyChangeOp.Remove, mapping),
            ) if special(mapping) =>
          (head, mid, tail :+ tx)
        // filter out head cases
        case ((head, mid, tail), (tx, TopologyChangeOp.Add, mapping)) if special(mapping) =>
          (head :+ tx, mid, tail)
        // always include domain trust participant certs (preserve their order)
        case (
              (head, mid, tail),
              (tx, _anyOpIsGood, ParticipantState(side, dId, `participant`, _, _)),
            ) if domainId == dId && side != RequestSide.To =>
          (head :+ tx, mid, tail)
        // rest goes to mid
        case ((head, mid, tail), (tx, _, _)) => (head, mid :+ tx, tail)
      } match {
      case (head, mid, tail) if (head.size + mid.size + tail.size) < sender.maxBatchSize =>
        // send as one tx to save some startup time in our tests
        (head ++ mid ++ tail, Seq.empty, Seq.empty)
      // if the size of the batch is too large, keep the split
      case x => x
    }
  }

  private def bootstrapAndDispatch(
      tracedTransaction: Traced[
        NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]]
      ]
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    tracedTransaction withTraceContext { implicit traceContext => transactions =>
      val flushToParticipantET: EitherT[FutureUnlessShutdown, String, Option[ParticipantId]] =
        transactions.last1.transaction.transaction.element.mapping match {
          case ParticipantState(_, _, participant, _, _) =>
            for {
              catchupForParticipantO <- EitherT.right(
                performUnlessClosingF(functionFullName)(
                  catchup
                    .determineCatchupForParticipant(
                      transactions.head1.validFrom.value,
                      transactions.last1.validFrom.value,
                      participant,
                      protocolVersion,
                    )
                )
              )
              // if we crash during dispatching, we will resend all these transactions again
              // to the participant. this will for now produce some ugly warnings, but the
              // bootstrapping transactions will just be deduplicated at the transaction processor
              // with 3.x, the catchup / bootstrapping will not be necessary as participants
              // will download it directly from the sequencers
              catchupNE = catchupForParticipantO.flatMap { case (initial, txs) =>
                NonEmpty.from(txs.result).map(NE => (initial, NE))
              }
              _ <- catchupNE.fold(EitherT.rightT[FutureUnlessShutdown, String](())) {
                case (initial, txs) =>
                  // determine correct snapshot to sign this batch
                  // if the node is new, then the batch must be signed with the current key, as the snapshot will not contain
                  // old keys. if the participant has been there before, we need to use the last key the participant has known
                  val sp =
                    if (initial) {
                      logger.debug(
                        s"Using current authorized snapshot from=${transactions.head1.validFrom.value} for signing messages"
                      )
                      authorizedCryptoSnapshot(transactions.head1.validFrom.value)
                    } else {
                      logger.debug(
                        s"Using catchup authorized snapshot from=${txs.head1.validFrom.value} for signing messages"
                      )
                      authorizedCryptoSnapshot(txs.head1.validFrom.value)
                    }
                  // split the topology transactions such that the first message is self-consistent
                  val (head, mid, tail) = splitCatchupTransactionsForParticipant(
                    participant,
                    txs,
                  )
                  // send the catchup packs sequentially
                  val sentET: EitherT[FutureUnlessShutdown, String, Unit] =
                    MonadUtil.sequentialTraverse_(Seq((head, false), (mid, true), (tail, false))) {
                      case (txs, batching) if txs.nonEmpty =>
                        sender.sendTransactions(
                          sp,
                          txs,
                          Set(participant),
                          s"catchup-${if (batching) "tail" else "head"}",
                          batching,
                        )
                      case _ => EitherT.pure(())
                    }
                  sentET.flatMap { _ =>
                    // if the catchup contained a topology manager key change, we actuallly need to wait for it
                    // to become effective on the participant before we can proceed
                    waitIfTopologyManagerKeyWasRolled(txs)
                  }
              }
            } yield catchupForParticipantO.map(_ => participant)
          case _ =>
            EitherT.rightT(None)
        }
      val checkForMediatorActivationET: EitherT[FutureUnlessShutdown, String, Option[MediatorId]] =
        transactions.last1.transaction.transaction.element.mapping match {
          case MediatorDomainState(_, _, mediator) =>
            EitherT.right(
              performUnlessClosingF(functionFullName)(
                catchup
                  .determinePermissionChangeForMediator(
                    transactions.last1.validFrom.value,
                    mediator,
                  )
                  .map {
                    case (true, true) => None
                    case (false, true) => Some(mediator)
                    case (false, false) => None
                    case (true, false) =>
                      // TODO(#1251) implement catchup for mediator
                      logger.warn(
                        s"Mediator ${mediator} is deactivated and will miss out on topology transactions. This will break it"
                      )
                      None
                  }
              )
            )
          case _ =>
            EitherT.rightT(None)
        }
      for {
        includeParticipant <- flushToParticipantET
        includeMediator <- checkForMediatorActivationET
        _ <- sendTransactions(
          transactions,
          includeParticipant.toList ++ includeMediator.toList,
        )
        // update watermark, which we can as we successfully registered all transactions with the domain
        // we don't need to wait until they are processed
        _ <- EitherT.right(
          performUnlessClosingF(functionFullName)(
            targetStore.updateDispatchingWatermark(transactions.last1.validFrom.value)
          )
        )
        _ <- waitIfTopologyManagerKeyWasRolled(transactions)
      } yield ()
    }
  }

  private def sendTransactions(
      transactions: NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]],
      add: Seq[Member],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    require(transactions.size <= sender.maxBatchSize)
    val headSnapshot = authorizedStoreSnapshot(transactions.head1.validFrom.value)
    val receivingParticipantsF = performUnlessClosingF(functionFullName)(
      headSnapshot
        .participants()
        .map(_.collect {
          case (participantId, perm)
              if perm.isActive ||
                // with protocol version v5, we also dispatch topology transactions to disabled participants
                // which avoids the "catchup" tx computation.
                // but beware, there is a difference between "Disabled" and "None". with disabled, you are
                // explicitly disabled, with None, we are back at the behaviour of < v5
                protocolVersion >= ProtocolVersion.v5 =>
            participantId
        })
    )
    val mediatorsF = performUnlessClosingF(functionFullName)(
      headSnapshot.mediatorGroups().map(_.flatMap(_.active))
    )
    for {
      receivingParticipants <- EitherT.right(receivingParticipantsF)
      mediators <- EitherT.right(mediatorsF)
      _ <- sender.sendTransactions(
        authorizedCryptoSnapshot(transactions.head1.validFrom.value),
        transactions.map(_.transaction).toList,
        (receivingParticipants ++ staticDomainMembers ++ mediators ++ add).toSet,
        "normal",
      )
    } yield ()
  }

  override protected def onClosed(): Unit =
    Lifecycle.close(
      topologyManagerSequencerCounterTrackerStore,
      targetStore,
      authorizedStore,
      sender,
    )(logger)

}

private[domain] object DomainTopologyDispatcher {
  def create(
      domainId: DomainId,
      domainTopologyManager: DomainTopologyManager,
      targetClient: DomainTopologyClientWithInit,
      processor: TopologyTransactionProcessor,
      initialKeys: Map[KeyOwner, Seq[PublicKey]],
      targetStore: TopologyStore[TopologyStoreId.DomainStore],
      client: SequencerClient,
      timeTracker: DomainTimeTracker,
      crypto: Crypto,
      clock: Clock,
      addressSequencerAsDomainMember: Boolean,
      parameters: CantonNodeParameters,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      topologyManagerSequencerCounterTrackerStore: SequencerCounterTrackerStore,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[DomainTopologyDispatcher] = {

    val sender = new DomainTopologySender.Impl(
      domainId,
      domainTopologyManager.protocolVersion,
      client,
      timeTracker,
      clock,
      maxBatchSize = 100,
      retryInterval = 10.seconds,
      parameters.processingTimeouts,
      loggerFactory,
    )
    val dispatcher = new DomainTopologyDispatcher(
      domainId,
      domainTopologyManager.protocolVersion,
      domainTopologyManager.store,
      processor,
      initialKeys,
      targetStore,
      crypto,
      clock,
      addressSequencerAsDomainMember,
      parameters,
      futureSupervisor,
      sender,
      loggerFactory,
      topologyManagerSequencerCounterTrackerStore,
    )

    domainTopologyManager.addObserver(dispatcher)
    dispatcher
      .init(
        flushSequencerWithTimeProof(
          clock,
          timeTracker,
          targetClient,
          loggerFactory.getTracedLogger(DomainTopologyDispatcher.getClass),
        )
      )
      .map { _ =>
        dispatcher
      }

  }

  private def flushSequencerWithTimeProof(
      clock: Clock,
      timeTracker: DomainTimeTracker,
      targetClient: DomainTopologyClient,
      logger: TracedLogger,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] = {
    val now = clock.now
    def go(target: CantonTimestamp): Unit = {
      timeTracker
        .fetchTimeProof()
        .map { proof =>
          if (proof.timestamp < target) {
            if (clock.isSimClock) {
              go(target)
            } else {
              clock.scheduleAfter(_ => go(target), java.time.Duration.ofMillis(500))
            }
          }
        }
        .discard
    }
    for {
      // flush the sequencer client with a time-proof. this should ensure that we give the
      // topology processor time to catch up with any pending submissions
      //
      // This is not fool-proof as the time proof may be processed by a different sequencer replica
      // than where earlier transactions have been submitted and therefore overtake them.
      timestamp <- timeTracker.fetchTimeProof().map(_.timestamp)
      targetTimestamp = now.max(timestamp)
      // kick off background polling to ensure we don't get stuck
      _ = go(targetTimestamp)
      // wait until the topology client has seen this timestamp or our current clock
      // we need our current clock if we are lagging behind a lot (e.g. replaying after backup)
      _ = logger.info(s"Waiting for sequencer ts=${targetTimestamp} with current=$timestamp")
      _ <- targetClient
        .awaitTimestampUS(targetTimestamp, waitForEffectiveTime = false)
        .getOrElse(FutureUnlessShutdown.unit)
    } yield ()
  }

}

trait DomainTopologySender
    extends CloseableHealthComponent
    with AtomicHealthComponent
    with NamedLogging {
  def maxBatchSize: Int
  def sendTransactions(
      snapshot: DomainSnapshotSyncCryptoApi,
      transactions: Seq[GenericSignedTopologyTransaction],
      recipients: Set[Member],
      name: String,
      batching: Boolean = true,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit]
}

object DomainTopologySender extends TopologyDispatchingErrorGroup {

  class Impl(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      client: SequencerClient,
      timeTracker: DomainTimeTracker,
      clock: Clock,
      val maxBatchSize: Int,
      retryInterval: FiniteDuration,
      val timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext)
      extends DomainTopologySender {

    override val name: String = TopologyManagementInitialization.topologySenderHealthName
    override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()

    private val currentJob =
      new AtomicReference[Option[Promise[UnlessShutdown[Either[String, Unit]]]]](None)

    def sendTransactions(
        snapshot: DomainSnapshotSyncCryptoApi,
        transactions: Seq[GenericSignedTopologyTransaction],
        recipients: Set[Member],
        name: String,
        batching: Boolean,
    )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
      val nonEmptyGroup: Option[NonEmpty[Seq[NonEmpty[Set[Member]]]]] = NonEmpty
        .from(recipients.toSeq.map(rec => NonEmpty.mk(Set, rec)))
      val recipientGroup = nonEmptyGroup.map(x => Recipients.groups(x))
      def genBatch(
          batch: Seq[GenericSignedTopologyTransaction],
          recipients: Recipients,
      )(maxSequencingTime: CantonTimestamp) =
        DomainTopologyTransactionMessage
          .create(batch.toList, snapshot, domainId, Some(maxSequencingTime), protocolVersion)
          .map(batchMessage =>
            Batch(
              List(
                OpenEnvelope(batchMessage, recipients)(protocolVersion)
              ),
              protocolVersion,
            )
          )

      for {
        nonEmptyRecipients <- EitherT.fromOption[FutureUnlessShutdown](
          recipientGroup,
          show"Empty set of recipients for ${transactions}",
        )
        _ <- transactions
          .grouped(if (batching) maxBatchSize else transactions.size + 1)
          .zipWithIndex
          .foldLeft(EitherT.pure[FutureUnlessShutdown, String](())) { case (res, (batch, idx)) =>
            val offset = idx * maxBatchSize
            res.flatMap(_ =>
              for {
                _ <- ensureDelivery(
                  ts => genBatch(batch, nonEmptyRecipients)(ts),
                  s"${offset + 1} to ${offset + batch.size} out of ${transactions.size} ${name} topology transactions for ${recipients.size} recipients",
                )
              } yield ()
            )
          }
      } yield ()
    }

    private def finalizeCurrentJob(outcome: UnlessShutdown[Either[String, Unit]]): Unit = {
      currentJob.getAndSet(None).foreach { promise =>
        promise.trySuccess(outcome).discard
      }
    }

    override def onClosed(): Unit = {
      // invoked once all performUnlessClosed tasks are done.
      // due to our retry scheduling using the clock, we might not get to complete the promise
      finalizeCurrentJob(UnlessShutdown.AbortedDueToShutdown)
      Lifecycle.close(timeTracker, client)(logger)
    }

    protected def send(
        batch: CantonTimestamp => EitherT[Future, String, Batch[
          OpenEnvelope[DomainTopologyTransactionMessage]
        ]],
        callback: SendCallback,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Either[SendAsyncClientError, Unit]] =
      performUnlessClosingF(functionFullName)(
        (for {
          // add max-sequencing time to message to prevent replay attacks.
          // on the recipient side, we'll check for validity of these timestamp
          message <- batch(client.generateMaxSequencingTime)
            .leftMap(RequestInvalid)
          _ <- client.sendAsync(message, SendType.Other, callback = callback)
        } yield ()).value
      )

    private def ensureDelivery(
        batch: CantonTimestamp => EitherT[Future, String, Batch[
          OpenEnvelope[DomainTopologyTransactionMessage]
        ]],
        message: String,
    )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {

      val promise = Promise[UnlessShutdown[Either[String, Unit]]]()
      // as we care a lot about the order of the transactions, we should never get
      // concurrent invocations of this here
      ErrorUtil.requireArgument(
        currentJob.getAndSet(Some(promise)).isEmpty,
        "Ensure delivery called while we already had a pending call!",
      )

      def stopDispatching(str: String): Unit = {
        finalizeCurrentJob(UnlessShutdown.Outcome(Left(str)))
      }

      def abortDueToShutdown(): Unit = {
        logger.info("Aborting batch dispatching due to shutdown")
        finalizeCurrentJob(UnlessShutdown.AbortedDueToShutdown)
      }

      lazy val callback: SendCallback = {
        case UnlessShutdown.Outcome(_: SendResult.Success) =>
          if (getState.isOk) resolveUnhealthy()
          logger.debug(s"Successfully registered $message with the sequencer.")
          finalizeCurrentJob(UnlessShutdown.Outcome(Right(())))
        case UnlessShutdown.Outcome(SendResult.Error(error)) =>
          TopologyDispatchingInternalError.SendResultError(error).discard
          stopDispatching("Stopping due to an internal send result error")
        case UnlessShutdown.Outcome(_: SendResult.Timeout) =>
          degradationOccurred(TopologyDispatchingDegradation.SendTrackerTimeout())
          dispatch()
        case UnlessShutdown.AbortedDueToShutdown =>
          abortDueToShutdown()
      }

      def dispatch(): Unit = {
        logger.debug(s"Attempting to dispatch ${message}")
        FutureUtil.doNotAwait(
          send(batch, callback).transform {
            case x @ Success(UnlessShutdown.Outcome(Left(RequestRefused(error))))
                if error.category.retryable.nonEmpty =>
              degradationOccurred(TopologyDispatchingDegradation.SendRefused(error))
              // delay the retry, assuming that the retry is much smaller than our shutdown interval
              // the perform unless closing will ensure we don't close the ec before the delay
              // has kicked in
              logger.debug(s"Scheduling topology dispatching retry in ${retryInterval}")
              clock
                .scheduleAfter(_ => dispatch(), java.time.Duration.ofMillis(retryInterval.toMillis))
                .discard
              x
            case x @ Success(UnlessShutdown.Outcome(Left(error))) =>
              TopologyDispatchingInternalError.AsyncResultError(error).discard
              stopDispatching("Stopping due to an unexpected async result error")
              x
            case x @ Success(UnlessShutdown.Outcome(Right(_))) =>
              // nice, the sequencer seems to be accepting our request
              x
            case x @ Success(UnlessShutdown.AbortedDueToShutdown) =>
              abortDueToShutdown()
              x
            case x @ Failure(ex) =>
              TopologyDispatchingInternalError.UnexpectedException(ex).discard
              stopDispatching("Stopping due to an unexpected exception")
              x
          }.unwrap,
          "dispatch future threw an unexpected exception",
        )
      }

      dispatch()
      EitherT(FutureUnlessShutdown(promise.future))
    }

  }

  trait TopologyDispatchingDegradation extends CantonError

  @Explanation(
    "This warning occurs when the topology dispatcher experiences timeouts while trying to register topology transactions."
  )
  @Resolution(
    "This error should normally self-recover due to retries. If issue persist, contact support and investigate system state."
  )
  object TopologyDispatchingDegradation
      extends ErrorCode(
        id = "TOPOLOGY_DISPATCHING_DEGRADATION",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {
    final case class SendTrackerTimeout()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "The submitted domain topology transactions never appeared on the domain. Will try again."
        )
        with TopologyDispatchingDegradation
    final case class SendRefused(err: SendAsyncError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Did not succeed to register the domain topology transactions with the domain. Will try again."
        )
        with TopologyDispatchingDegradation
  }

  @Explanation(
    "This error is emitted if there is a fundamental, un-expected situation during topology dispatching. " +
      "In such a situation, the topology state of a newly onboarded participant or of all domain members might become outdated"
  )
  @Resolution(
    "Contact support."
  )
  object TopologyDispatchingInternalError
      extends ErrorCode(
        id = "TOPOLOGY_DISPATCHING_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class SendResultError(err: DeliverError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Received an unexpected deliver error during topology dispatching."
        )
    final case class AsyncResultError(err: SendAsyncClientError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Received an unexpected send async client error during topology dispatching."
        )
    final case class UnexpectedException(throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Send async threw an unexpected exception during topology dispatching.",
          throwableO = Some(throwable),
        )
  }

}

class MemberTopologyCatchup(
    store: TopologyStore[TopologyStoreId.AuthorizedStore],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** determine the set of transactions that needs to be sent to this participant
    *
    * participants that were disabled or are joining new need to get an initial set
    * of topology transactions.
    *
    * @param asOf timestamp of the topology transaction in the authorized store that potentially activated a participant
    * @param upToExclusive timestamp up to which we have to send the topology transaction
    * @param participantId the participant of interest
    */
  def determineCatchupForParticipant(
      upToExclusive: CantonTimestamp,
      asOf: CantonTimestamp,
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[(Boolean, StoredTopologyTransactions[TopologyChangeOp])]] = {
    def isActivation(
        from: Option[ParticipantPermission],
        to: Option[ParticipantPermission],
    ): Boolean =
      if (protocolVersion < ProtocolVersion.v5) {
        // before protocol version 5, we would not dispatch topology changes to disabled participants.
        // starting with v5, we do, which changes the transition logic here
        !from.getOrElse(ParticipantPermission.Disabled).isActive && to
          .getOrElse(ParticipantPermission.Disabled)
          .isActive
      } else {
        from.isEmpty && to.nonEmpty
      }
    determinePermissionChange(asOf, participantId).flatMap {
      case (from, to) if isActivation(from, to) =>
        for {
          participantInactiveSince <- findDeactivationTsOfParticipantBefore(
            asOf,
            participantId,
            protocolVersion,
          )
          participantCatchupTxs <- store.findTransactionsInRange(
            // exclusive, as last message a participant will receive is the one that is deactivating it
            asOfExclusive = participantInactiveSince.getOrElse(CantonTimestamp.MinValue),
            // exclusive as the participant will receive the rest subsequently
            upToExclusive = upToExclusive,
          )
        } yield {
          val cleanedTxs = deduplicateDomainParameterChanges(participantCatchupTxs)
          logger.info(
            s"Participant $participantId is changing from ${from} to ${to}, requires to catch up ${cleanedTxs.result.size}"
          )
          Option.when(cleanedTxs.result.nonEmpty)((participantInactiveSince.isEmpty, cleanedTxs))
        }
      case _ => Future.successful(None)
    }
  }

  /** deduplicate domain parameter changes
    *
    * we anyway only need the most recent one.
    */
  private def deduplicateDomainParameterChanges(
      original: StoredTopologyTransactions[TopologyChangeOp]
  ): StoredTopologyTransactions[TopologyChangeOp] = {
    val start = (
      None: Option[StoredTopologyTransaction[TopologyChangeOp]],
      Seq.empty[StoredTopologyTransaction[TopologyChangeOp]],
    )
    // iterate over result set and keep last dpc
    val (dpcO, rest) = original.result.foldLeft(start) {
      case ((_, acc), elem)
          if elem.transaction.transaction.element.mapping.dbType == DomainTopologyTransactionType.DomainParameters =>
        (Some(elem), acc) // override previous dpc
      case ((cur, acc), elem) => (cur, acc :+ elem)
    }
    // append dpc
    val result = dpcO.fold(rest)(rest :+ _)
    StoredTopologyTransactions(result)
  }

  private def findDeactivationTsOfParticipantBefore(
      ts: CantonTimestamp,
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = {
    def isDeactivation(
        from: Option[ParticipantPermission],
        to: Option[ParticipantPermission],
    ): Boolean = {
      // same as above: different logic depending on protocol version
      if (protocolVersion < ProtocolVersion.v5) {
        from.getOrElse(ParticipantPermission.Disabled).isActive && !to
          .getOrElse(ParticipantPermission.Disabled)
          .isActive
      } else {
        from.nonEmpty && to.isEmpty
      }
    }

    val limit = 1000
    // let's find the last time this node got deactivated. note, we assume that the participant is
    // not active as of ts
    store
      .findTsOfParticipantStateChangesBefore(ts, participantId, limit)
      .flatMap {
        // if there is no participant state change, then just return min value
        case Nil => Future.successful(None)
        case some =>
          // list is ordered desc
          some.toList
            .findM { ts =>
              determinePermissionChange(ts, participantId)
                .map { case (pre, post) => isDeactivation(pre, post) }
            }
            .flatMap {
              // found last point in time
              case Some(resTs) => Future.successful(Some(resTs))
              // we found many transactions but no deactivation, keep on looking
              // this should only happen if we had 1000 times a participant state update that never deactivated
              // a node. unlikely but could happen
              case None if some.length == limit =>
                findDeactivationTsOfParticipantBefore(
                  // continue from lowest ts here
                  some.lastOption.getOrElse(CantonTimestamp.MinValue), // is never empty
                  participantId,
                  protocolVersion,
                )
              case None => Future.successful(None)
            }
      }
  }

  private def determinePermissionChange(
      timestamp: CantonTimestamp,
      participant: ParticipantId,
  ): Future[(Option[ParticipantPermission], Option[ParticipantPermission])] =
    determineChange(
      timestamp,
      _.findParticipantState(participant)
        .map(_.map(_.permission)),
    )

  private def determineChange[T](
      timestamp: CantonTimestamp,
      get: StoreBasedTopologySnapshot => Future[T],
  ): Future[(T, T)] = {
    // as topology timestamps are "as of exclusive", if we want to access the impact
    // of a topology tx, we need to look at ts + immediate successor
    val curF = get(snapshot(timestamp.immediateSuccessor))
    val prevF = get(snapshot(timestamp))
    prevF.zip(curF)
  }

  private def snapshot(timestamp: CantonTimestamp): StoreBasedTopologySnapshot =
    new StoreBasedTopologySnapshot(
      timestamp,
      store,
      Map(),
      false,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      loggerFactory,
    )

  def determinePermissionChangeForMediator(
      timestamp: CantonTimestamp,
      mediator: MediatorId,
  ): Future[(Boolean, Boolean)] = determineChange(timestamp, _.isMediatorActive(mediator))

}
