// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.IncreaseOfLedgerTimeRecordTimeTolerance
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

trait TopologyManagerObserver {
  def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

class DomainTopologyManager(
    clock: Clock,
    crypto: Crypto,
    override val store: TopologyStore[DomainStore],
    val outboxQueue: DomainOutboxQueue,
    enableTopologyTransactionValidation: Boolean,
    protocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[DomainStore](
      clock,
      crypto,
      store,
      TopologyManager.PV(protocolVersion),
      timeouts,
      futureSupervisor,
      loggerFactory,
    ) {
  override protected val processor: TopologyStateProcessor =
    new TopologyStateProcessor(
      store,
      Some(outboxQueue),
      enableTopologyTransactionValidation,
      new ValidatingTopologyMappingChecks(store, loggerFactory),
      crypto.pureCrypto,
      loggerFactory,
    )

  // When evaluating transactions against the domain store, we want to validate against
  // the head state. We need to take all previously sequenced transactions into account, because
  // we don't know when the submitted transaction actually gets sequenced.
  override def timestampForValidation(): CantonTimestamp = CantonTimestamp.MaxValue
}

class AuthorizedTopologyManager(
    clock: Clock,
    crypto: Crypto,
    store: TopologyStore[AuthorizedStore],
    enableTopologyTransactionValidation: Boolean,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[AuthorizedStore](
      clock,
      crypto,
      store,
      TopologyManager.NoPV,
      timeouts,
      futureSupervisor,
      loggerFactory,
    ) {
  override protected val processor: TopologyStateProcessor =
    new TopologyStateProcessor(
      store,
      None,
      enableTopologyTransactionValidation,
      NoopTopologyMappingChecks,
      crypto.pureCrypto,
      loggerFactory,
    )

  // for the authorized store, we take the next unique timestamp, because transactions
  // are directly persisted into the store.
  override def timestampForValidation(): CantonTimestamp = clock.uniqueTime()
}

abstract class TopologyManager[+StoreID <: TopologyStoreId](
    val clock: Clock,
    val crypto: Crypto,
    val store: TopologyStore[StoreID],
    val managerVersion: TopologyManager.Version,
    val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManagerStatus
    with NamedLogging
    with FlagCloseable {

  def timestampForValidation(): CantonTimestamp

  // sequential queue to run all the processing that does operate on the state
  protected val sequentialQueue = new SimpleExecutionQueue(
    "topology-manager-x-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  protected val processor: TopologyStateProcessor

  override def queueSize: Int = sequentialQueue.queueSize

  private val observers = new AtomicReference[Seq[TopologyManagerObserver]](Seq.empty)
  def addObserver(observer: TopologyManagerObserver): Unit =
    observers.updateAndGet(_ :+ observer).discard

  def removeObserver(observer: TopologyManagerObserver): Unit =
    observers.updateAndGet(_.filterNot(_ == observer)).discard

  def clearObservers(): Unit = observers.set(Seq.empty)

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param op              the operation that should be performed
    * @param mapping         the mapping that should be added
    * @param signingKeys     the key which should be used to sign
    * @param protocolVersion the protocol version corresponding to the transaction
    * @param expectFullAuthorization whether the transaction must be fully signed and authorized by keys on this node
    * @param force           force dangerous operations, such as removing the last signing key of a participant
    * @return the domain state (initialized or not initialized) or an error code of why the addition failed
    */
  def proposeAndAuthorize(
      op: TopologyChangeOp,
      mapping: TopologyMapping,
      serial: Option[PositiveInt],
      signingKeys: Seq[Fingerprint],
      protocolVersion: ProtocolVersion,
      expectFullAuthorization: Boolean,
      force: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, GenericSignedTopologyTransaction] = {
    logger.debug(show"Attempting to build, sign, and ${op} ${mapping} with serial $serial")
    for {
      tx <- build(op, mapping, serial, protocolVersion, signingKeys).mapK(
        FutureUnlessShutdown.outcomeK
      )
      signedTx <- signTransaction(tx, signingKeys, isProposal = !expectFullAuthorization)
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- add(Seq(signedTx), force = force, expectFullAuthorization)
    } yield signedTx
  }

  /** Authorizes an existing topology transaction by signing it and adding it to the topology state.
    * If {@code expectFullAuthorization} is {@code true} and the topology transaction cannot be fully
    * authorized with keys from this node, returns with an error and the existing topology transaction
    * remains unchanged.
    *
    * @param transactionHash the uniquely identifying hash of a previously proposed topology transaction
    * @param signingKeys the key which should be used to sign
    * @param force force dangerous operations, such as removing the last signing key of a participant
    * @param expectFullAuthorization whether the resulting transaction must be fully authorized or not
    * @return the signed transaction or an error code of why the addition failed
    */
  def accept(
      transactionHash: TxHash,
      signingKeys: Seq[Fingerprint],
      force: Boolean,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, GenericSignedTopologyTransaction] = {
    val effective = EffectiveTime(clock.now)
    for {
      transactionsForHash <- EitherT
        .right[TopologyManagerError](
          store.findTransactionsByTxHash(effective, Set(transactionHash))
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      existingTransaction <-
        EitherT.fromEither[FutureUnlessShutdown][
          TopologyManagerError,
          GenericSignedTopologyTransaction,
        ](transactionsForHash match {
          case Seq(tx) => Right(tx)
          case Seq() =>
            Left(
              TopologyManagerError.TopologyTransactionNotFound.Failure(transactionHash, effective)
            )
          case tooManyActiveTransactionsWithSameHash =>
            // TODO(#12390) proper error
            Left(
              TopologyManagerError.InternalError.ImplementMe(
                s"found too many transactions for txHash=$transactionsForHash: $tooManyActiveTransactionsWithSameHash"
              )
            )
        })
      extendedTransaction <- extendSignature(existingTransaction, signingKeys).mapK(
        FutureUnlessShutdown.outcomeK
      )
      _ <- add(
        Seq(extendedTransaction),
        force = force,
        expectFullAuthorization = expectFullAuthorization,
      )
    } yield {
      extendedTransaction
    }
  }

  def build[Op <: TopologyChangeOp, M <: TopologyMapping](
      op: Op,
      mapping: M,
      serial: Option[PositiveInt],
      protocolVersion: ProtocolVersion,
      newSigningKeys: Seq[Fingerprint],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, TopologyTransaction[Op, M]] = {
    for {
      existingTransactions <- EitherT.right(
        store.findTransactionsForMapping(EffectiveTime.MaxValue, NonEmpty(Set, mapping.uniqueKey))
      )
      _ = if (existingTransactions.sizeCompare(1) > 0)
        logger.warn(
          s"found more than one valid mapping for unique key ${mapping.uniqueKey} of type ${mapping.code}"
        )
      existingTransaction = existingTransactions
        .sortBy(_.serial)
        .lastOption
        .map(t => (t.operation, t.mapping, t.serial, t.signatures))

      // If the same operation and mapping is proposed repeatedly, insist that
      // new keys are being added. Otherwise reject consistently with daml 2.x-based topology management.
      _ <- existingTransaction match {
        case Some((`op`, `mapping`, _, existingSignatures)) if op == TopologyChangeOp.Replace =>
          EitherT.cond[Future][TopologyManagerError, Unit](
            (newSigningKeys.toSet -- existingSignatures.map(_.signedBy).toSet).nonEmpty,
            (),
            TopologyManagerError.MappingAlreadyExists
              .Failure(mapping, existingSignatures.map(_.signedBy)),
          )
        case _ => EitherT.rightT[Future, TopologyManagerError](())
      }

      theSerial <- ((existingTransaction, serial) match {
        case (None, None) =>
          // auto-select 1
          EitherT.rightT(PositiveInt.one)
        case (None, Some(proposed)) =>
          // didn't find an existing transaction, therefore the proposed serial must be 1
          EitherT.cond[Future][TopologyManagerError, PositiveInt](
            proposed == PositiveInt.one,
            PositiveInt.one,
            TopologyManagerError.SerialMismatch.Failure(PositiveInt.one, proposed),
          )

        // TODO(#12390) existing mapping and the proposed mapping are the same. does this only add a (superfluous) signature?
        //              maybe we should reject this proposal, but for now we need this to pass through successfully, because we don't
        //              support proper topology transaction validation yet, especially not for multi-sig transactions.
        case (Some((`op`, `mapping`, existingSerial, _)), None) =>
          // auto-select existing
          EitherT.rightT(existingSerial)
        case (Some((`op`, `mapping`, existingSerial, _)), Some(proposed)) =>
          EitherT.cond[Future](
            existingSerial == proposed,
            existingSerial,
            TopologyManagerError.SerialMismatch.Failure(existingSerial, proposed),
          )

        case (Some((_, _, existingSerial, _)), None) =>
          // auto-select existing+1
          EitherT.rightT(existingSerial.increment)
        case (Some((_, _, existingSerial, _)), Some(proposed)) =>
          // check that the proposed serial matches existing+1
          val next = existingSerial.increment
          EitherT.cond[Future](
            next == proposed,
            next,
            TopologyManagerError.SerialMismatch.Failure(next, proposed),
          )
      }): EitherT[Future, TopologyManagerError, PositiveInt]
    } yield TopologyTransaction(op, theSerial, mapping, protocolVersion)
  }

  def signTransaction[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signingKeys: Seq[Fingerprint],
      isProposal: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, SignedTopologyTransaction[Op, M]] = {
    for {
      // find signing keys.
      keys <- (signingKeys match {
        case first +: rest =>
          // TODO(#12945) should we check whether this node could sign with keys that are required in addition to the ones provided in signingKeys, and fetch those keys?
          EitherT.pure(NonEmpty.mk(Set, first, rest*))
        case _empty =>
          // TODO(#12945) get signing keys for transaction.
          EitherT.leftT(
            TopologyManagerError.InternalError.ImplementMe(
              "Automatic signing key lookup not yet implemented. Please specify a signing key explicitly."
            )
          )
      }): EitherT[Future, TopologyManagerError, NonEmpty[Set[Fingerprint]]]
      // create signed transaction
      signed <- SignedTopologyTransaction
        .create(
          transaction,
          keys,
          isProposal,
          crypto.privateCrypto,
          // TODO(#14048) The `SignedTopologyTransactionX` may use a different versioning scheme than the contained transaction. Use the right protocol version here
          transaction.representativeProtocolVersion.representative,
        )
        .leftMap {
          case SigningError.UnknownSigningKey(keyId) =>
            TopologyManagerError.SecretKeyNotInStore.Failure(keyId)
          case err => TopologyManagerError.InternalError.TopologySigningError(err)
        }: EitherT[Future, TopologyManagerError, SignedTopologyTransaction[Op, M]]
    } yield signed
  }

  def extendSignature[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: SignedTopologyTransaction[Op, M],
      signingKey: Seq[Fingerprint],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, SignedTopologyTransaction[Op, M]] = {
    for {
      // find signing keys
      keys <- (signingKey match {
        case keys @ (_first +: _rest) =>
          // TODO(#12945) filter signing keys relevant for the required authorization for this transaction
          EitherT.rightT(keys.toSet)
        case _ =>
          // TODO(#12945) fetch signing keys that are relevant for the required authorization for this transaction
          EitherT.leftT(
            TopologyManagerError.InternalError.ImplementMe(
              "Automatic signing key lookup not yet implemented. Please specify a signing explicitly."
            )
          )
      }): EitherT[Future, TopologyManagerError, Set[Fingerprint]]
      signatures <- keys.toSeq.parTraverse(
        crypto.privateCrypto
          .sign(transaction.hash.hash, _)
          .leftMap(err =>
            TopologyManagerError.InternalError.TopologySigningError(err): TopologyManagerError
          )
      )
    } yield transaction.addSignatures(signatures)
  }

  /** sequential(!) adding of topology transactions
    *
    * @param force force a dangerous change (such as revoking the last key)
    */
  def add(
      transactions: Seq[GenericSignedTopologyTransaction],
      force: Boolean,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    sequentialQueue.executeE(
      {
        val ts = timestampForValidation()
        for {
          _ <- MonadUtil.sequentialTraverse_(transactions)(transactionIsNotDangerous(_, force))
          transactionsInStore <- EitherT.liftF(
            store.findTransactionsByTxHash(
              EffectiveTime.MaxValue,
              transactions.map(_.hash).toSet,
            )
          )
          existingHashes = transactionsInStore
            .map(tx => tx.hash -> tx.hashOfSignatures)
            .toMap
          (existingTransactions, newTransactionsOrAdditionalSignatures) = transactions.partition(
            tx => existingHashes.get(tx.hash).contains(tx.hashOfSignatures)
          )
          _ = logger.debug(
            s"Processing ${newTransactionsOrAdditionalSignatures.size}/${transactions.size} non-duplicate transactions"
          )
          _ = if (existingTransactions.nonEmpty) {
            logger.debug(
              s"Ignoring existing transactions: $existingTransactions"
            )
          }

          _ <-
            if (newTransactionsOrAdditionalSignatures.isEmpty)
              EitherT.pure[Future, TopologyManagerError](())
            else {
              // validate incrementally and apply to in-memory state
              processor
                .validateAndApplyAuthorization(
                  SequencedTime(ts),
                  EffectiveTime(ts),
                  newTransactionsOrAdditionalSignatures,
                  abortIfCascading = !force,
                  expectFullAuthorization,
                )
                .leftFlatMap(rejectedTransactions =>
                  // a "duplicate rejection" is not a reason to report an error as it's just a no-op
                  EitherT.fromEither[Future](
                    rejectedTransactions
                      .flatMap(_.nonDuplicateRejectionReason)
                      .headOption
                      .map(_.toTopologyManagerError)
                      // if the only rejections where duplicates (i.e. headOption returns None),
                      // we filter them out and proceed with all other validated transactions, because the
                      // TopologyStateProcessor will have treated them as such as well.
                      // this is similar to how duplicate transactions are not considered failures in processor.validateAndApplyAuthorization
                      .toLeft(rejectedTransactions.filter(tx => tx.rejectionReason.isEmpty))
                  )
                )
                .map { acceptedTransactions =>
                  notifyObservers(ts, acceptedTransactions.map(_.transaction))
                }
            }
        } yield ()
      },
      "add-topology-transaction",
    )

  private def transactionIsNotDangerous(
      transaction: SignedTopologyTransaction[TopologyChangeOp, TopologyMapping],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, Unit] = transaction.mapping match {
    case DomainParametersState(domainId, newDomainParameters) =>
      checkLedgerTimeRecordTimeToleranceNotIncreasing(domainId, newDomainParameters, force)
    case _ => EitherT.rightT(())
  }

  private def checkLedgerTimeRecordTimeToleranceNotIncreasing(
      domainId: DomainId,
      newDomainParameters: DynamicDomainParameters,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] = {
    // See i9028 for a detailed design.

    EitherT(for {
      headTransactions <- store.findPositiveTransactions(
        asOf = CantonTimestamp.MaxValue,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(DomainParametersState.code),
        filterUid = Some(Seq(domainId.uid)),
        filterNamespace = None,
      )
    } yield {
      headTransactions.toTopologyState
        .collectFirst { case DomainParametersState(_, previousParameters) =>
          previousParameters
        } match {
        case None => Right(())
        case Some(domainParameters) =>
          val changeIsDangerous =
            newDomainParameters.ledgerTimeRecordTimeTolerance > domainParameters.ledgerTimeRecordTimeTolerance
          if (changeIsDangerous && force) {
            logger.info(
              s"Forcing dangerous increase of ledger time record time tolerance from ${domainParameters.ledgerTimeRecordTimeTolerance} to ${newDomainParameters.ledgerTimeRecordTimeTolerance}"
            )
          }
          Either.cond(
            !changeIsDangerous || force,
            (),
            IncreaseOfLedgerTimeRecordTimeTolerance.TemporarilyInsecure(
              domainParameters.ledgerTimeRecordTimeTolerance,
              newDomainParameters.ledgerTimeRecordTimeTolerance,
            ),
          )
      }
    })
  }

  /** notify observers about new transactions about to be stored */
  protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = Future
    .sequence(
      observers
        .get()
        .map(_.addedNewTransactions(timestamp, transactions).onShutdown(()))
    )
    .map(_ => ())

  override protected def onClosed(): Unit = Lifecycle.close(store, sequentialQueue)(logger)

  override def toString: String = s"TopologyManager[${store.storeId}]"
}

object TopologyManager {
  sealed trait Version {
    def validation: ProtocolVersionValidation
    def serialization: ProtocolVersion
  }
  final case class PV(pv: ProtocolVersion) extends Version {
    override def validation: ProtocolVersionValidation = ProtocolVersionValidation.PV(pv)
    override def serialization: ProtocolVersion = pv
  }

  final case object NoPV extends Version {
    override def validation: ProtocolVersionValidation = ProtocolVersionValidation.NoValidation
    override def serialization: ProtocolVersion = ProtocolVersion.latest
  }
}
