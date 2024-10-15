// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.{
  DangerousCommandRequiresForce,
  IncreaseOfLedgerTimeRecordTimeTolerance,
  ParticipantTopologyManagerError,
}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyManagerSigningKeyDetection,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil, SimpleExecutionQueue}
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
    nodeId: UniqueIdentifier,
    clock: Clock,
    crypto: Crypto,
    staticDomainParameters: StaticDomainParameters,
    override val store: TopologyStore[DomainStore],
    val outboxQueue: DomainOutboxQueue,
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[DomainStore, DomainCryptoPureApi](
      nodeId,
      clock,
      crypto,
      store,
      TopologyManager.PV(staticDomainParameters.protocolVersion),
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      futureSupervisor,
      loggerFactory,
    ) {
  override protected val processor: TopologyStateProcessor[DomainCryptoPureApi] =
    new TopologyStateProcessor[DomainCryptoPureApi](
      store,
      Some(outboxQueue),
      new ValidatingTopologyMappingChecks(store, loggerFactory),
      new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto),
      loggerFactory,
    )

  // When evaluating transactions against the domain store, we want to validate against
  // the head state. We need to take all previously sequenced transactions into account, because
  // we don't know when the submitted transaction actually gets sequenced.
  override def timestampForValidation(): CantonTimestamp = CantonTimestamp.MaxValue

  override protected def validateTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)]] = {
    val ts = timestampForValidation()
    processor
      .validateAndApplyAuthorization(
        SequencedTime(ts),
        EffectiveTime(ts),
        transactions,
        expectFullAuthorization,
      )
      .map(txs => Seq(txs -> ts))
  }
}

class AuthorizedTopologyManager(
    nodeId: UniqueIdentifier,
    clock: Clock,
    crypto: Crypto,
    store: TopologyStore[AuthorizedStore],
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[AuthorizedStore, CryptoPureApi](
      nodeId,
      clock,
      crypto,
      store,
      TopologyManager.NoPV,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      futureSupervisor,
      loggerFactory,
    ) {
  override protected val processor: TopologyStateProcessor[CryptoPureApi] =
    new TopologyStateProcessor[CryptoPureApi](
      store,
      None,
      NoopTopologyMappingChecks,
      crypto.pureCrypto,
      loggerFactory,
    )

  // for the authorized store, we take the next unique timestamp, because transactions
  // are directly persisted into the store.
  override def timestampForValidation(): CantonTimestamp = clock.uniqueTime()

  // In the authorized store, we validate transactions individually to allow them to be dispatched correctly regardless
  // of the batch size configured in the outbox
  override protected def validateTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)]] =
    MonadUtil
      .sequentialTraverse(transactions) { transaction =>
        val ts = timestampForValidation()
        processor
          .validateAndApplyAuthorization(
            SequencedTime(ts),
            EffectiveTime(ts),
            Seq(transaction),
            expectFullAuthorization,
          )
          .map(_ -> ts)
      }
}

abstract class TopologyManager[+StoreID <: TopologyStoreId, +PureCrypto <: CryptoPureApi](
    val nodeId: UniqueIdentifier,
    val clock: Clock,
    val crypto: Crypto,
    val store: TopologyStore[StoreID],
    val managerVersion: TopologyManager.Version,
    exitOnFatalFailures: Boolean,
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
    crashOnFailure = exitOnFatalFailures,
  )

  protected val processor: TopologyStateProcessor[PureCrypto]

  override def queueSize: Int = sequentialQueue.queueSize

  private val observers = new AtomicReference[Seq[TopologyManagerObserver]](Seq.empty)
  def addObserver(observer: TopologyManagerObserver): Unit =
    observers.updateAndGet(_ :+ observer).discard

  def removeObserver(observer: TopologyManagerObserver): Unit =
    observers.updateAndGet(_.filterNot(_ == observer)).discard

  def clearObservers(): Unit = observers.set(Seq.empty)

  /** Allows the participant to override this method to enable additional checks on the VettedPackages transaction.
    * Only the participant has access to the package store.
    */
  def validatePackageVetting(
      currentlyVettedPackages: Set[LfPackageId],
      nextPackageIds: Set[LfPackageId],
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val _ = traceContext
    EitherT.rightT(())
  }

  def checkCannotDisablePartyWithActiveContracts(partyId: PartyId, forceFlags: ForceFlags)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    traceContext.discard
    EitherT.rightT(())
  }

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param op              the operation that should be performed
    * @param mapping         the mapping that should be added
    * @param signingKeys     the key which should be used to sign
    * @param protocolVersion the protocol version corresponding to the transaction
    * @param expectFullAuthorization whether the transaction must be fully signed and authorized by keys on this node
    * @param forceChanges    force dangerous operations, such as removing the last signing key of a participant
    * @return the domain state (initialized or not initialized) or an error code of why the addition failed
    */
  def proposeAndAuthorize(
      op: TopologyChangeOp,
      mapping: TopologyMapping,
      serial: Option[PositiveInt],
      signingKeys: Seq[Fingerprint],
      protocolVersion: ProtocolVersion,
      expectFullAuthorization: Boolean,
      forceChanges: ForceFlags = ForceFlags.none,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, GenericSignedTopologyTransaction] = {
    logger.debug(show"Attempting to build, sign, and $op $mapping with serial $serial")
    for {
      existingTransaction <- findExistingTransaction(mapping).mapK(
        FutureUnlessShutdown.outcomeK
      )
      tx <- build(op, mapping, serial, protocolVersion, existingTransaction).mapK(
        FutureUnlessShutdown.outcomeK
      )
      signedTx <- signTransaction(
        tx,
        signingKeys,
        isProposal = !expectFullAuthorization,
        protocolVersion,
        existingTransaction,
        forceChanges,
      )
      _ <- add(Seq(signedTx), forceChanges, expectFullAuthorization)
    } yield signedTx
  }

  /** Authorizes an existing topology transaction by signing it and adding it to the topology state.
    * If {@code expectFullAuthorization} is {@code true} and the topology transaction cannot be fully
    * authorized with keys from this node, returns with an error and the existing topology transaction
    * remains unchanged.
    *
    * @param transactionHash the uniquely identifying hash of a previously proposed topology transaction
    * @param signingKeys the key which should be used to sign
    * @param forceChanges force dangerous operations, such as removing the last signing key of a participant
    * @param expectFullAuthorization whether the resulting transaction must be fully authorized or not
    * @return the signed transaction or an error code of why the addition failed
    */
  def accept(
      transactionHash: TxHash,
      signingKeys: Seq[Fingerprint],
      forceChanges: ForceFlags,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, GenericSignedTopologyTransaction] = {
    val effective = EffectiveTime(clock.now)
    for {
      transactionsForHash <- EitherT
        .right[TopologyManagerError](
          store.findTransactionsAndProposalsByTxHash(effective, Set(transactionHash))
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
            Left(
              TopologyManagerError.TooManyTransactionsWithHash
                .Failure(transactionHash, effective, tooManyActiveTransactionsWithSameHash)
            )
        })
      extendedTransaction <- extendSignature(existingTransaction, signingKeys, forceChanges)
      _ <- add(
        Seq(extendedTransaction),
        expectFullAuthorization = expectFullAuthorization,
        forceChanges = forceChanges,
      )
    } yield {
      extendedTransaction
    }
  }

  def findExistingTransaction[M <: TopologyMapping](mapping: M)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, Option[GenericSignedTopologyTransaction]] =
    for {
      existingTransactions <- EitherT.right(
        store.findTransactionsForMapping(EffectiveTime.MaxValue, NonEmpty(Set, mapping.uniqueKey))
      )
      _ = if (existingTransactions.sizeCompare(1) > 0)
        logger.warn(
          s"found more than one valid mapping for unique key ${mapping.uniqueKey} of type ${mapping.code}"
        )
    } yield existingTransactions.maxByOption(_.serial)

  def build[Op <: TopologyChangeOp, M <: TopologyMapping](
      op: Op,
      mapping: M,
      serial: Option[PositiveInt],
      protocolVersion: ProtocolVersion,
      existingTransaction: Option[GenericSignedTopologyTransaction],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, TopologyTransaction[Op, M]] = {
    val existingTransactionTuple =
      existingTransaction.map(t => (t.operation, t.mapping, t.serial, t.signatures))
    for {
      theSerial <- ((existingTransactionTuple, serial) match {
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
        // The stored mapping and the proposed mapping are the same. This likely only adds an additional signature.
        // If not, then duplicates will be filtered out down the line.
        case (Some((`op`, `mapping`, existingSerial, _)), None) =>
          // auto-select existing
          EitherT.rightT(existingSerial)
        case (Some((`op`, `mapping`, existingSerial, signatures)), Some(proposed)) =>
          EitherT.cond[Future](
            existingSerial == proposed,
            existingSerial,
            TopologyManagerError.MappingAlreadyExists.Failure(mapping, signatures.map(_.signedBy)),
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
      protocolVersion: ProtocolVersion,
      existingTransaction: Option[GenericSignedTopologyTransaction],
      forceChanges: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, SignedTopologyTransaction[Op, M]] = {
    val existingTransactionTuple =
      existingTransaction.map(t => (t.operation, t.mapping, t.serial, t.signatures))
    val transactionOp = transaction.operation
    val transactionMapping = transaction.mapping
    for {
      // find signing keys.
      keysToUseForSigning <- determineKeysToUse(transaction, signingKeys, forceChanges)
      // If the same operation and mapping is proposed repeatedly, insist that
      // new keys are being added. Otherwise, reject consistently with daml 2.x-based topology management.
      _ <- existingTransactionTuple match {
        case Some((`transactionOp`, `transactionMapping`, _, existingSignatures)) =>
          EitherT.cond[FutureUnlessShutdown][TopologyManagerError, Unit](
            (keysToUseForSigning -- existingSignatures
              .map(_.signedBy)
              .toSet).nonEmpty,
            (),
            TopologyManagerError.MappingAlreadyExists
              .Failure(transactionMapping, existingSignatures.map(_.signedBy)),
          )
        case _ => EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())
      }
      // create signed transaction
      signed <- SignedTopologyTransaction
        .create(
          transaction,
          keysToUseForSigning,
          isProposal,
          crypto.privateCrypto,
          protocolVersion,
        )
        .leftMap {
          case SigningError.UnknownSigningKey(keyId) =>
            TopologyManagerError.SecretKeyNotInStore.Failure(keyId)
          case err => TopologyManagerError.InternalError.TopologySigningError(err)
        }: EitherT[FutureUnlessShutdown, TopologyManagerError, SignedTopologyTransaction[Op, M]]
    } yield signed
  }

  def extendSignature[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: SignedTopologyTransaction[Op, M],
      signingKeys: Seq[Fingerprint],
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, SignedTopologyTransaction[Op, M]] =
    for {
      // find signing keys
      keys <- determineKeysToUse(transaction.transaction, signingKeys, forceFlags)
      signatures <- keys.forgetNE.toSeq.parTraverse(
        crypto.privateCrypto
          .sign(transaction.hash.hash, _)
          .leftMap(err =>
            TopologyManagerError.InternalError.TopologySigningError(err): TopologyManagerError
          )
      )
    } yield transaction.addSignatures(signatures)

  private def determineKeysToUse(
      transaction: GenericTopologyTransaction,
      signingKeysToUse: Seq[Fingerprint],
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, NonEmpty[Set[Fingerprint]]] =
    NonEmpty.from(signingKeysToUse.toSet) match {
      // the caller has specified at least 1 key to be used
      case Some(explicitlySpecifiedKeysToUse) =>
        val useForce = forceFlags.permits(ForceFlag.AllowUnvalidatedSigningKeys)
        for {
          usableKeys <- loadValidSigningKeys(transaction, returnAllValidKeys = true)
          unusableKeys = explicitlySpecifiedKeysToUse.toSet -- usableKeys

          // log that the force flag overrides unusable keys
          _ = if (unusableKeys.nonEmpty && useForce) {
            logger.info(
              s"ForceFlag permits usage of keys not suitable for the signing the transaction: $unusableKeys"
            )
          } // no need to log other cases, because the error is anyway logged properly

          // check that all explicitly specified keys are valid (or force)
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            unusableKeys.isEmpty || useForce,
            TopologyManagerError.NoAppropriateSigningKeyInStore
              .Failure(unusableKeys.toSeq): TopologyManagerError,
          )
          // We only get here if there are no unusable keys or the caller has used the force flag to use them anyway
        } yield explicitlySpecifiedKeysToUse

      // the caller has not specified any keys to be used.
      // therefore let's determine the most "specific" set of keys that are known to this node for signing
      case None =>
        for {
          detectedKeysToUse <- loadValidSigningKeys(transaction, returnAllValidKeys = false)
          keysNE <- EitherT.fromOption[FutureUnlessShutdown](
            NonEmpty.from(detectedKeysToUse.toSet),
            TopologyManagerError.NoAppropriateSigningKeyInStore
              .Failure(Seq.empty): TopologyManagerError,
          )
        } yield keysNE

    }

  private def loadValidSigningKeys(
      transaction: GenericTopologyTransaction,
      returnAllValidKeys: Boolean,
  )(implicit traceContext: TraceContext) =
    for {
      existing <- EitherT
        .right[TopologyManagerError](
          store.findTransactionsForMapping(
            EffectiveTime(timestampForValidation()),
            NonEmpty(Set, transaction.mapping.uniqueKey),
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      result <- new TopologyManagerSigningKeyDetection(
        store,
        crypto.pureCrypto,
        crypto.cryptoPrivateStore,
        loggerFactory,
      )
        .getValidSigningKeysForTransaction(
          timestampForValidation(),
          transaction,
          existing.headOption.map(_.transaction), // there should be at most one entry
          returnAllValidKeys,
        )
    } yield result

  protected def validateTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)]]

  /** sequential(!) adding of topology transactions
    *
    * @param forceChanges force a dangerous change (such as revoking the last key)
    */
  def add(
      transactions: Seq[GenericSignedTopologyTransaction],
      forceChanges: ForceFlags,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    sequentialQueue.executeEUS(
      for {
        _ <- MonadUtil.sequentialTraverse_(transactions)(
          transactionIsNotDangerous(_, forceChanges)
        )

        transactionsInStore <- EitherT
          .liftF(
            store.findTransactionsAndProposalsByTxHash(
              EffectiveTime.MaxValue,
              transactions.map(_.hash).toSet,
            )
          )
          .mapK(FutureUnlessShutdown.outcomeK)
        existingHashes = transactionsInStore
          .map(tx => tx.hash -> tx.hashOfSignatures)
          .toMap
        (existingTransactions, newTransactionsOrAdditionalSignatures) = transactions.partition(tx =>
          existingHashes.get(tx.hash).contains(tx.hashOfSignatures)
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
            EitherT.pure[FutureUnlessShutdown, TopologyManagerError](())
          else {
            // validate incrementally and apply to in-memory state
            EitherT
              .right[TopologyManagerError](
                validateTransactions(
                  newTransactionsOrAdditionalSignatures,
                  expectFullAuthorization,
                )
              )
              .flatMap(filterDuplicatesAndNotify)
              .mapK(FutureUnlessShutdown.outcomeK)
          }
      } yield (),
      "add-topology-transaction",
    )

  private def filterDuplicatesAndNotify(
      validatedTxWithTimestamps: Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)]
  )(implicit traceContext: TraceContext) = {
    val nonDuplicateRejectionReasons =
      validatedTxWithTimestamps.flatMap(_._1.flatMap(_.nonDuplicateRejectionReason))
    val firstError = nonDuplicateRejectionReasons.headOption.map(_.toTopologyManagerError)
    EitherT(
      // if the only rejections where duplicates (i.e. headOption returns None),
      // we filter them out and proceed with all other validated transactions, because the
      // TopologyStateProcessor will have treated them as such as well.
      // this is similar to how duplicate transactions are not considered failures in processor.validateAndApplyAuthorization
      firstError
        .toLeft(validatedTxWithTimestamps)
        .traverse { transactionsList =>
          // notify observers about the transactions
          MonadUtil.sequentialTraverse(transactionsList) { case (transactions, ts) =>
            notifyObservers(ts, transactions.filter(_.rejectionReason.isEmpty).map(_.transaction))
          }
        }
    )
  }

  private def transactionIsNotDangerous(
      transaction: SignedTopologyTransaction[TopologyChangeOp, TopologyMapping],
      forceChanges: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = transaction.mapping match {
    case DomainParametersState(domainId, newDomainParameters) =>
      checkLedgerTimeRecordTimeToleranceNotIncreasing(domainId, newDomainParameters, forceChanges)
    case OwnerToKeyMapping(member, _) =>
      checkTransactionIsForCurrentNode(member, forceChanges, transaction.mapping.code)
    case VettedPackages(participantId, newPackages) =>
      checkPackageVettingIsNotDangerous(
        participantId,
        newPackages.map(_.packageId).toSet,
        forceChanges,
        transaction.mapping.code,
      )
    case PartyToParticipant(partyId, _, participants) =>
      checkPartyToParticipantIsNotDangerous(
        partyId,
        participants,
        forceChanges,
        transaction.transaction.operation,
      )
    case _ => EitherT.rightT(())
  }

  private def checkTransactionIsForCurrentNode(
      member: Member,
      forceChanges: ForceFlags,
      topologyMappingCode: TopologyMapping.Code,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    EitherTUtil.condUnitET(
      member.uid == nodeId || forceChanges.permits(ForceFlag.AlienMember),
      DangerousCommandRequiresForce.AlienMember(member, topologyMappingCode),
    )

  private def checkLedgerTimeRecordTimeToleranceNotIncreasing(
      domainId: DomainId,
      newDomainParameters: DynamicDomainParameters,
      forceChanges: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    // See i9028 for a detailed design.

    EitherT(for {
      headTransactions <- FutureUnlessShutdown.outcomeF(
        store.findPositiveTransactions(
          asOf = CantonTimestamp.MaxValue,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(DomainParametersState.code),
          filterUid = Some(Seq(domainId.uid)),
          filterNamespace = None,
        )
      )
    } yield {
      headTransactions
        .collectOfMapping[DomainParametersState]
        .collectLatestByUniqueKey
        .toTopologyState
        .collectFirst { case DomainParametersState(_, previousParameters) =>
          previousParameters
        } match {
        case None => Right(())
        case Some(domainParameters) =>
          val changeIsDangerous =
            newDomainParameters.ledgerTimeRecordTimeTolerance > domainParameters.ledgerTimeRecordTimeTolerance
          val force = forceChanges.permits(ForceFlag.LedgerTimeRecordTimeToleranceIncrease)
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

  private def checkPackageVettingIsNotDangerous(
      participantId: Member,
      newPackageIds: Set[LfPackageId],
      forceChanges: ForceFlags,
      topologyMappingCode: TopologyMapping.Code,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    for {
      currentlyVettedPackages <- EitherT
        .right(
          store
            .findPositiveTransactions(
              asOf = CantonTimestamp.MaxValue,
              asOfInclusive = false,
              isProposal = false,
              types = Seq(VettedPackages.code),
              filterUid = Some(Seq(participantId.uid)),
              filterNamespace = None,
            )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .map {
          _.collectOfMapping[VettedPackages].collectLatestByUniqueKey.toTopologyState
            .collectFirst { case VettedPackages(_, existingPackageIds) =>
              existingPackageIds.map(_.packageId)
            }
            .getOrElse(Nil)
            .toSet
        }
      _ <- checkPackageVettingRevocation(currentlyVettedPackages, newPackageIds, forceChanges)
      _ <- checkTransactionIsForCurrentNode(participantId, forceChanges, topologyMappingCode)
      _ <- validatePackageVetting(currentlyVettedPackages, newPackageIds, forceChanges)
    } yield ()

  private def checkPackageVettingRevocation(
      currentlyVettedPackages: Set[LfPackageId],
      nextPackageIds: Set[LfPackageId],
      forceChanges: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val removed = currentlyVettedPackages -- nextPackageIds
    val force = forceChanges.permits(ForceFlag.AllowUnvetPackage)
    val changeIdDangerous = removed.nonEmpty
    EitherT.cond(
      !changeIdDangerous || force,
      (),
      ParticipantTopologyManagerError.DangerousVettingCommandsRequireForce.Reject(),
    )
  }

  private def checkPartyToParticipantIsNotDangerous(
      partyId: PartyId,
      nextParticipants: Seq[HostingParticipant],
      forceChanges: ForceFlags,
      operation: TopologyChangeOp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val removedParticipantIds = operation match {
      case TopologyChangeOp.Replace =>
        store
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(PartyToParticipant.code),
            filterUid = Some(Seq(partyId.uid)),
            filterNamespace = None,
          )
          .map {
            _.collectOfMapping[PartyToParticipant].collectLatestByUniqueKey.toTopologyState
              .collectFirst { case PartyToParticipant(_, _, currentHostingParticipants) =>
                currentHostingParticipants.map(_.participantId.uid).toSet -- nextParticipants.map(
                  _.participantId.uid
                )
              }
              .getOrElse(Set.empty)
          }
      case TopologyChangeOp.Remove =>
        Future.successful(
          nextParticipants.map(_.participantId.uid).toSet
        )
    }

    for {
      removed <- EitherT.right(removedParticipantIds).mapK(FutureUnlessShutdown.outcomeK)
      _ <-
        if (removed.contains(nodeId)) {
          checkCannotDisablePartyWithActiveContracts(partyId, forceChanges: ForceFlags)
        } else EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())

    } yield ()
  }

  /** notify observers about new transactions about to be stored */
  private def notifyObservers(
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
