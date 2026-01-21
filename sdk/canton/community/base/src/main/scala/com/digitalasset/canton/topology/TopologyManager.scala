// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.TopologyManager.assignExpectedUsageToKeys
import com.digitalasset.canton.topology.TopologyManagerError.{
  DangerousCommandRequiresForce,
  IncreaseOfPreparationTimeRecordTimeTolerance,
  InvalidSynchronizerSuccessor,
  TooManyPendingTopologyTransactions,
  ValueOutOfBounds,
}
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyManagerSigningKeyDetection,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.{
  AuthorizedStore,
  SynchronizerStore,
  TemporaryStore,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.{
  TimeQuery,
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Observation
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.RequiredAuth
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.checks.{
  NoopTopologyMappingChecks,
  OptionalTopologyMappingChecks,
  RequiredTopologyMappingChecks,
  TopologyMappingChecks,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{LfPackageId, config}

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.unused
import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

trait TopologyManagerObserver {
  def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

class SynchronizerTopologyManager(
    nodeId: UniqueIdentifier,
    clock: Clock,
    crypto: SynchronizerCrypto,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    topologyCacheAggregatorConfig: BatchAggregatorConfig,
    topologyConfig: TopologyConfig,
    override val store: TopologyStore[SynchronizerStore],
    val outboxQueue: SynchronizerOutboxQueue,
    dispatchQueueBackpressureLimit: NonNegativeInt,
    disableOptionalTopologyChecks: Boolean,
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[SynchronizerStore, SynchronizerCrypto](
      nodeId,
      clock,
      crypto,
      store,
      TopologyManager.PV(staticSynchronizerParameters.protocolVersion),
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      futureSupervisor,
      loggerFactory,
    ) {
  def psid: PhysicalSynchronizerId = store.storeId.psid

  override def noBackpressure(): Boolean =
    outboxQueue.numUnsentTransactions < dispatchQueueBackpressureLimit.value

  override protected val processor: TopologyStateProcessor = {

    def makeChecks(lookup: TopologyStateLookup): TopologyMappingChecks = {
      val required =
        RequiredTopologyMappingChecks(Some(staticSynchronizerParameters), lookup, loggerFactory)

      if (!disableOptionalTopologyChecks)
        new TopologyMappingChecks.All(
          required,
          new OptionalTopologyMappingChecks(store, loggerFactory),
        )
      else required
    }
    TopologyStateProcessor.forTopologyManager(
      store,
      topologyCacheAggregatorConfig,
      topologyConfig,
      Some(outboxQueue),
      makeChecks,
      crypto.pureCrypto,
      timeouts,
      loggerFactory,
    )
  }

  // When evaluating transactions against the synchronizer store, we want to validate against
  // the head state. We need to take all previously sequenced transactions into account, because
  // we don't know when the submitted transaction actually gets sequenced.
  override def timestampForValidation()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    store
      .maxTimestamp(SequencedTime.MaxValue, includeRejected = false)
      .map(
        _.map { case (_sequenced, effective) =>
          // use the immediate successor of the highest effective time, so that
          // lookups in the store find all the transactions up to that timestamp
          effective.value.immediateSuccessor
        }.getOrElse(CantonTimestamp.MaxValue)
      )

  override protected def validateTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    (Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)], AsyncResult[Unit])
  ] = for {
    ts <- timestampForValidation()
    validationResult <- processor
      .validateAndApplyAuthorization(
        SequencedTime(ts),
        EffectiveTime(ts),
        transactions,
        expectFullAuthorization = expectFullAuthorization,
        // the synchronizer topology manager does not permit weaker validation checks,
        // because these transactions would be rejected during the validating after sequencing.
        relaxChecksForBackwardsCompatibility = false,
      )
  } yield {
    val (txs, asyncResult) = validationResult
    (Seq(txs -> ts), asyncResult)
  }
}

class TemporaryTopologyManager(
    nodeId: UniqueIdentifier,
    clock: Clock,
    crypto: Crypto,
    topologyCacheAggregatorConfig: BatchAggregatorConfig,
    topologyConfig: TopologyConfig,
    store: TopologyStore[TemporaryStore],
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LocalTopologyManager(
      nodeId,
      clock,
      crypto,
      topologyCacheAggregatorConfig,
      topologyConfig,
      store,
      exitOnFatalFailures = false,
      timeouts,
      futureSupervisor,
      loggerFactory,
    ) {
  override def noBackpressure(): Boolean = true

}

class AuthorizedTopologyManager(
    nodeId: UniqueIdentifier,
    clock: Clock,
    crypto: Crypto,
    topologyCacheAggregatorConfig: BatchAggregatorConfig,
    topologyConfig: TopologyConfig,
    store: TopologyStore[AuthorizedStore],
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LocalTopologyManager(
      nodeId,
      clock,
      crypto,
      topologyCacheAggregatorConfig,
      topologyConfig,
      store,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      futureSupervisor,
      loggerFactory,
    ) {
  def initialize(implicit @unused traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit

  override def noBackpressure(): Boolean = true
}

abstract class LocalTopologyManager[StoreId <: TopologyStoreId](
    nodeId: UniqueIdentifier,
    clock: Clock,
    crypto: Crypto,
    topologyCacheAggregatorConfig: BatchAggregatorConfig,
    topologyConfig: TopologyConfig,
    store: TopologyStore[StoreId],
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[StoreId, Crypto](
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
  override protected val processor: TopologyStateProcessor =
    TopologyStateProcessor.forTopologyManager(
      store,
      topologyCacheAggregatorConfig,
      topologyConfig,
      None,
      _ => NoopTopologyMappingChecks,
      crypto.pureCrypto,
      timeouts,
      loggerFactory,
    )

  // for the authorized store, we take the next unique timestamp, because transactions
  // are directly persisted into the store.
  override def timestampForValidation()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    FutureUnlessShutdown.pure(clock.uniqueTime())

  // In the authorized store, we validate transactions individually to allow them to be dispatched correctly regardless
  // of the batch size configured in the outbox
  override protected def validateTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    (Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)], AsyncResult[Unit])
  ] =
    MonadUtil
      .sequentialTraverse(transactions) { transaction =>
        for {
          ts <- timestampForValidation()
          validationResult <- processor
            .validateAndApplyAuthorization(
              SequencedTime(ts),
              EffectiveTime(ts),
              Seq(transaction),
              expectFullAuthorization = expectFullAuthorization,
              // we allow importing older topology state such as OTK with missing signing key signatures into a temporary topology store,
              // so that we can import legacy OTKs for debugging/investigation purposes
              relaxChecksForBackwardsCompatibility = store.storeId.isTemporaryStore,
            )

        } yield {
          val (txs, asyncResult) = validationResult
          ((txs, ts), asyncResult)
        }
      }
      .map { txs =>
        val (txsAndTimestamp, asyncResults) = txs.unzip
        (txsAndTimestamp, asyncResults.combineAll)
      }
}

/** @param crypto
  *   We use a type parameter [[com.digitalasset.canton.crypto.BaseCrypto]] because it can either be
  *   extended from a [[LocalTopologyManager]], etc. which does not need to account for static
  *   synchronizer parameters, or a [[SynchronizerTopologyManager]], which does need to account for
  *   them.
  */
abstract class TopologyManager[+StoreID <: TopologyStoreId, +CryptoType <: BaseCrypto](
    val nodeId: UniqueIdentifier,
    val clock: Clock,
    val crypto: CryptoType,
    val store: TopologyStore[StoreID],
    val managerVersion: TopologyManager.Version,
    exitOnFatalFailures: Boolean,
    val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManagerStatus
    with NamedLogging
    with FlagCloseable
    with HasCloseContext {

  /** The timestamp that will be used for validating the topology transactions before submitting
    * them for sequencing to a synchronizer or storing it in the local store.
    */
  def timestampForValidation()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp]

  // sequential queue to run all the processing that does operate on the state
  protected val sequentialQueue = new SimpleExecutionQueue(
    "topology-manager-x-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  /** must return true if this topology manager should be backpressured */
  protected def noBackpressure(): Boolean
  protected val processor: TopologyStateProcessor

  override def queueSize: Int = sequentialQueue.queueSize

  private val observers = new AtomicReference[Seq[TopologyManagerObserver]](Seq.empty)
  def addObserver(observer: TopologyManagerObserver): Unit =
    observers.updateAndGet(_ :+ observer).discard

  def removeObserver(observer: TopologyManagerObserver): Unit =
    observers.updateAndGet(_.filterNot(_ == observer)).discard

  def clearObservers(): Unit = observers.set(Seq.empty)

  /** Allows the participant to override this method to enable additional checks on the
    * VettedPackages transaction. Only the participant has access to the package store.
    */
  def validatePackageVetting(
      @unused currentlyVettedPackages: Set[LfPackageId],
      @unused nextPackageIds: Set[LfPackageId],
      @unused dryRunSnapshot: Option[PackageMetadata],
      @unused forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val _ = traceContext
    EitherT.rightT(())
  }

  def checkCannotDisablePartyWithActiveContracts(
      @unused partyId: PartyId,
      @unused forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    traceContext.discard
    EitherT.rightT(())
  }

  private def checkConfirmingThresholdIsNotAboveNumberOfHostingNodes(
      threshold: PositiveInt,
      numberOfHostingNodes: Int,
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyManagerError](
      (numberOfHostingNodes == 0 || threshold.value <= numberOfHostingNodes || forceFlags.permits(
        ForceFlag.AllowConfirmingThresholdCanBeMet
      )),
      TopologyManagerError.ConfirmingThresholdCannotBeReached
        .Reject(threshold, numberOfHostingNodes),
    )

  def checkInsufficientSignatoryAssigningParticipantsForParty(
      @unused partyId: PartyId,
      @unused currentThreshold: PositiveInt,
      @unused nextThreshold: Option[PositiveInt],
      @unused nextConfirmingParticipants: Seq[HostingParticipant],
      @unused forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    traceContext.discard
    EitherT.rightT(())
  }

  def checkInsufficientParticipantPermissionForSignatoryParty(
      @unused party: PartyId,
      @unused forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    traceContext.discard
    EitherTUtil.unitUS
  }

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param op
    *   the operation that should be performed
    * @param mapping
    *   the mapping that should be added
    * @param signingKeys
    *   the keys which should be used to sign
    * @param protocolVersion
    *   the protocol version corresponding to the transaction
    * @param expectFullAuthorization
    *   whether the transaction must be fully signed and authorized by keys on this node
    * @param forceChanges
    *   force dangerous operations, such as removing the last signing key of a participant
    * @return
    *   the synchronizer state (initialized or not initialized) or an error code of why the addition
    *   failed
    */
  def proposeAndAuthorize(
      op: TopologyChangeOp,
      mapping: TopologyMapping,
      serial: Option[PositiveInt],
      signingKeys: Seq[Fingerprint],
      protocolVersion: ProtocolVersion,
      expectFullAuthorization: Boolean,
      forceChanges: ForceFlags = ForceFlags.none,
      waitToBecomeEffective: Option[config.NonNegativeFiniteDuration],
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TopologyManagerError,
    GenericSignedTopologyTransaction,
  ] = {
    val signingKeyString =
      if (signingKeys.nonEmpty)
        s"signed by ${signingKeys.mkString(", ")}"
      else ""
    logger.info(
      show"Attempting to build, sign, and $op $mapping with serial $serial $signingKeyString"
    )
    for {
      existingTransaction <- findExistingTransaction(mapping)
      tx <- build(op, mapping, serial, protocolVersion, existingTransaction)
      signedTx <- signTransaction(
        tx,
        signingKeys,
        isProposal = !expectFullAuthorization,
        protocolVersion,
        existingTransaction,
        forceChanges,
      )

      asyncResult <- add(Seq(signedTx), forceChanges, expectFullAuthorization)
      _ <- waitToBecomeEffective match {
        case Some(timeout) =>
          EitherT.pure[FutureUnlessShutdown, TopologyManagerError](
            timeout.awaitUS(s"proposeAndAuthorize-wait-for-effective")(asyncResult.unwrap)
          )
        case None => EitherTUtil.unitUS[TopologyManagerError]
      }
    } yield signedTx
  }

  /** Authorizes an existing topology transaction by signing it and adding it to the topology state.
    * If {@code expectFullAuthorization} is {@code true} and the topology transaction cannot be
    * fully authorized with keys from this node, returns with an error and the existing topology
    * transaction remains unchanged.
    *
    * @param transactionHash
    *   the uniquely identifying hash of a previously proposed topology transaction
    * @param signingKeys
    *   the key which should be used to sign
    * @param forceChanges
    *   force dangerous operations, such as removing the last signing key of a participant
    * @param expectFullAuthorization
    *   whether the resulting transaction must be fully authorized or not
    * @return
    *   the signed transaction or an error code of why the addition failed
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
          store.findLatestTransactionsAndProposalsByTxHash(Set(transactionHash))
        )
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
    } yield extendedTransaction
  }

  def findExistingTransaction[M <: TopologyMapping](mapping: M)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Option[GenericSignedTopologyTransaction]] =
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
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, TopologyTransaction[Op, M]] = {
    val existingTransactionTuple =
      existingTransaction.map(t => (t.operation, t.mapping, t.serial, t.signatures))
    for {
      theSerial <- ((existingTransactionTuple, serial) match {
        case (None, None) =>
          // auto-select 1
          EitherT.rightT(PositiveInt.one)
        case (None, Some(proposed)) =>
          // didn't find an existing transaction, therefore the proposed serial must be 1
          EitherT.cond[FutureUnlessShutdown][TopologyManagerError, PositiveInt](
            proposed == PositiveInt.one,
            PositiveInt.one,
            TopologyManagerError.SerialMismatch
              .Failure(actual = Some(proposed), expected = Some(PositiveInt.one)),
          )
        // The stored mapping and the proposed mapping are the same. This likely only adds an additional signature.
        // If not, then duplicates will be filtered out down the line.
        case (Some((`op`, `mapping`, existingSerial, _)), None) =>
          // auto-select existing
          EitherT.rightT(existingSerial)
        case (Some((`op`, `mapping`, existingSerial, signatures)), Some(proposed)) =>
          EitherT.cond[FutureUnlessShutdown](
            existingSerial == proposed,
            existingSerial,
            TopologyManagerError.MappingAlreadyExists
              .Failure(mapping, signatures.map(_.authorizingLongTermKey)),
          )

        case (Some((_, _, existingSerial, _)), None) =>
          // auto-select existing+1
          EitherT.rightT(existingSerial.increment)
        case (Some((_, _, existingSerial, _)), Some(proposed)) =>
          // check that the proposed serial matches existing+1
          val next = existingSerial.increment
          EitherT.cond[FutureUnlessShutdown](
            next == proposed,
            next,
            TopologyManagerError.SerialMismatch
              .Failure(actual = Some(proposed), expected = Some(next)),
          )
      }): EitherT[FutureUnlessShutdown, TopologyManagerError, PositiveInt]
    } yield TopologyTransaction(op, theSerial, mapping, protocolVersion)
  }

  private def signTransaction[Op <: TopologyChangeOp, M <: TopologyMapping](
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
              .map(_.authorizingLongTermKey)
              .toSet).nonEmpty,
            (),
            TopologyManagerError.MappingAlreadyExists
              .Failure(transactionMapping, existingSignatures.map(_.authorizingLongTermKey)),
          )
        case _ => EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())
      }
      signed <- SignedTopologyTransaction
        .signAndCreate(
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
      keysWithNoExistingSignature = keys.diff(transaction.signatures.map(_.authorizingLongTermKey))
      updatedSignedTransaction <- NonEmpty.from(keysWithNoExistingSignature) match {
        case Some(keysWithNoExistingSignatureNE) =>
          val keyWithUsage = assignExpectedUsageToKeys(
            transaction.mapping,
            keysWithNoExistingSignatureNE,
            forSigning = true,
          )
          keyWithUsage.toList.toNEF
            .parTraverse { case (key, usage) =>
              crypto.privateCrypto
                .sign(transaction.hash.hash, key, usage)
                .leftMap(err =>
                  TopologyManagerError.InternalError.TopologySigningError(err): TopologyManagerError
                )
            }
            .map(signatures => transaction.addSingleSignatures(signatures.fromNEF.toSet))

        case None =>
          logger.info("No keys available to this node can provide additional signatures.")
          EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](transaction)
      }
    } yield updatedSignedTransaction

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
          requiredAuthAndUsableKeys <- loadValidSigningKeys(
            transaction,
            returnAllValidKeys = true,
          )
          (requiredAuth, usableKeys) = requiredAuthAndUsableKeys
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
              .Failure(requiredAuth, unusableKeys.toSeq): TopologyManagerError,
          )
          // We only get here if there are no unusable keys or the caller has used the force flag to use them anyway
        } yield explicitlySpecifiedKeysToUse

      // the caller has not specified any keys to be used.
      // therefore let's determine the most "specific" set of keys that are known to this node for signing
      case None =>
        for {
          requiredAuthAndDetectedKeysToUse <- loadValidSigningKeys(
            transaction,
            returnAllValidKeys = false,
          )
          (requiredAuth, detectedKeysToUse) = requiredAuthAndDetectedKeysToUse
          keysNE <- EitherT.fromOption[FutureUnlessShutdown](
            NonEmpty.from(detectedKeysToUse.toSet),
            TopologyManagerError.NoAppropriateSigningKeyInStore
              .Failure(requiredAuth, Seq.empty): TopologyManagerError,
          )
        } yield keysNE

    }

  private def loadValidSigningKeys(
      transaction: GenericTopologyTransaction,
      returnAllValidKeys: Boolean,
  )(implicit traceContext: TraceContext) =
    for {
      ts <- EitherT.right[TopologyManagerError](timestampForValidation())
      existing <- EitherT
        .right[TopologyManagerError](
          store.findTransactionsForMapping(
            EffectiveTime(ts),
            NonEmpty(Set, transaction.mapping.uniqueKey),
          )
        )
      result <- new TopologyManagerSigningKeyDetection(
        store,
        crypto.pureCrypto,
        crypto.cryptoPrivateStore,
        loggerFactory,
      )
        .getValidSigningKeysForTransaction(
          ts,
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
  ): FutureUnlessShutdown[
    (Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)], AsyncResult[Unit])
  ]

  /** sequential(!) adding of topology transactions
    *
    * @param forceChanges
    *   force a dangerous change (such as revoking the last key)
    */
  def add(
      transactions: Seq[GenericSignedTopologyTransaction],
      forceChanges: ForceFlags,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, AsyncResult[Unit]] =
    sequentialQueue.executeEUS(
      for {
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          noBackpressure(),
          TooManyPendingTopologyTransactions.Backpressure(),
        )
        _ <- MonadUtil.sequentialTraverse_(transactions)(
          transactionIsNotDangerous(_, forceChanges)
        )

        transactionsInStore <- EitherT
          .liftF(store.findLatestTransactionsAndProposalsByTxHash(transactions.map(_.hash).toSet))

        existingHashes = transactionsInStore.map(tx => tx.hash -> tx).toMap

        // find transactions that provide new signatures
        (existingTransactions, newTransactionsOrAdditionalSignatures) = transactions.partition {
          tx =>
            existingHashes.get(tx.hash).exists { existingTx =>
              val newFingerprints = tx.signatures.map(_.authorizingLongTermKey)
              val existingFingerprints = existingTx.signatures.map(_.authorizingLongTermKey)

              /*
              Diff is done based on the fingerprint (signedBy) because signatures can be non-deterministic
              (e.g. with EC-DSA) where the same key produces a different signature for the same hash.
              This avoids ending up with several signatures for the same key.
               */
              newFingerprints.diff(existingFingerprints).isEmpty
            }
        }
        _ = logger.debug(
          s"Processing ${newTransactionsOrAdditionalSignatures.size}/${transactions.size} non-duplicate transactions"
        )
        _ = if (existingTransactions.nonEmpty) {
          logger.debug(
            s"Ignoring existing transactions: $existingTransactions"
          )
        }

        asyncResult <-
          if (newTransactionsOrAdditionalSignatures.isEmpty)
            EitherT.pure[FutureUnlessShutdown, TopologyManagerError](AsyncResult.immediate)
          else {
            // validate incrementally and apply to in-memory state
            for {
              transactionsAndTimestampAndAsyncResult <- EitherT
                .right[TopologyManagerError](
                  validateTransactions(
                    newTransactionsOrAdditionalSignatures,
                    expectFullAuthorization,
                  )
                )
              (transactionsAndTimestamp, asyncResult) = transactionsAndTimestampAndAsyncResult
              _ <- failOrNotifyObservers(transactionsAndTimestamp)
            } yield asyncResult
          }
      } yield asyncResult,
      "add-topology-transaction",
    )

  private def failOrNotifyObservers(
      validatedTxWithTimestamps: Seq[(Seq[GenericValidatedTopologyTransaction], CantonTimestamp)]
  )(implicit traceContext: TraceContext) = {
    val firstError =
      validatedTxWithTimestamps.iterator
        .flatMap { case (transactions, _ts) =>
          transactions.collectFirst { case ValidatedTopologyTransaction(_, Some(rejection), _) =>
            rejection.toTopologyManagerError
          }
        }
        .nextOption()
    EitherT(
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
    case SynchronizerParametersState(synchronizerId, newSynchronizerParameters) =>
      for {
        _ <- checkPreparationTimeRecordTimeToleranceNotIncreasing(
          synchronizerId,
          newSynchronizerParameters,
          forceChanges,
        )

        _ <- EitherT.fromEither[FutureUnlessShutdown](
          checkDynamicSynchronizerParametersBounds(newSynchronizerParameters, forceChanges)
        )
      } yield ()

    case OwnerToKeyMapping(member, _) =>
      checkTransactionIsForCurrentNode(member, forceChanges, transaction.mapping.code)

    case VettedPackages(participantId, newPackages) =>
      checkPackageVettingIsNotDangerous(
        participantId,
        newPackages.map(_.packageId).toSet,
        forceChanges,
        transaction.mapping.code,
      )

    case PartyToKeyMapping(_, SigningKeysWithThreshold(keys, threshold)) =>
      checkSigningThresholdCanBeReached(
        threshold,
        keys,
      )

    case PartyToParticipant(partyId, threshold, participants, signingKeysWithThreholdO) =>
      checkPartyToParticipantIsNotDangerous(
        partyId,
        threshold,
        participants,
        signingKeysWithThreholdO,
        forceChanges,
        transaction.transaction.operation,
      )

    case upgradeAnnouncement: SynchronizerUpgradeAnnouncement =>
      if (transaction.operation == TopologyChangeOp.Replace)
        checkSynchronizerUpgradeAnnouncementIsNotDangerous(upgradeAnnouncement, transaction.serial)
      else EitherT.pure(())

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

  private def checkDynamicSynchronizerParametersBounds(
      newSynchronizerParameters: DynamicSynchronizerParameters,
      forceChanges: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): Either[TopologyManagerError, Unit] =
    if (!forceChanges.permits(ForceFlag.AllowOutOfBoundsValue))
      TopologyManager.checkBounds(newSynchronizerParameters)
    else ().asRight

  private def checkPreparationTimeRecordTimeToleranceNotIncreasing(
      synchronizerId: SynchronizerId,
      newSynchronizerParameters: DynamicSynchronizerParameters,
      forceChanges: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    // See i9028 for a detailed design.

    EitherT(for {
      headTransactions <-
        store.findPositiveTransactions(
          asOf = CantonTimestamp.MaxValue,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(SynchronizerParametersState.code),
          filterUid = Some(NonEmpty(Seq, synchronizerId.uid)),
          filterNamespace = None,
        )
    } yield {
      headTransactions
        .collectOfMapping[SynchronizerParametersState]
        .collectLatestByUniqueKey
        .toTopologyState
        .collectFirst { case SynchronizerParametersState(_, previousParameters) =>
          previousParameters
        } match {
        case None => Either.unit
        case Some(synchronizerParameters) =>
          val changeIsDangerous =
            newSynchronizerParameters.preparationTimeRecordTimeTolerance > synchronizerParameters.preparationTimeRecordTimeTolerance
          val force = forceChanges.permits(ForceFlag.PreparationTimeRecordTimeToleranceIncrease)
          if (changeIsDangerous && force) {
            logger.info(
              s"Forcing dangerous increase of preparation time record time tolerance from ${synchronizerParameters.preparationTimeRecordTimeTolerance} to ${newSynchronizerParameters.preparationTimeRecordTimeTolerance}"
            )
          }
          Either.cond(
            !changeIsDangerous || force,
            (),
            IncreaseOfPreparationTimeRecordTimeTolerance.TemporarilyInsecure(
              synchronizerParameters.preparationTimeRecordTimeTolerance,
              newSynchronizerParameters.preparationTimeRecordTimeTolerance,
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
              filterUid = Some(NonEmpty(Seq, participantId.uid)),
              filterNamespace = None,
            )
        )
        .map {
          _.collectOfMapping[VettedPackages].collectLatestByUniqueKey.toTopologyState
            .collectFirst { case VettedPackages(_, existingPackageIds) =>
              existingPackageIds.map(_.packageId)
            }
            .getOrElse(Nil)
            .toSet
        }
      _ <- checkTransactionIsForCurrentNode(participantId, forceChanges, topologyMappingCode)
      _ <- validatePackageVetting(currentlyVettedPackages, newPackageIds, None, forceChanges)
    } yield ()

  private def checkSynchronizerUpgradeAnnouncementIsNotDangerous(
      upgradeAnnouncement: SynchronizerUpgradeAnnouncement,
      serial: PositiveInt,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {

    val resF = store
      .inspect(
        proposals = false,
        timeQuery = TimeQuery.Range(None, None),
        asOfExclusiveO = None,
        op = None,
        types = Seq(TopologyMapping.Code.SynchronizerUpgradeAnnouncement),
        idFilter = None,
        namespaceFilter = None,
      )
      .map { result =>
        result
          .collectOfMapping[SynchronizerUpgradeAnnouncement]
          .result
          .maxByOption(_.serial) match {
          case None => ().asRight

          case Some(latestUpgradeAnnouncement) =>
            // If the latest is another upgrade, we want the PSId to be strictly greater
            if (serial == latestUpgradeAnnouncement.serial)
              ().asRight
            else {
              val previouslyAnnouncedSuccessorPSId =
                latestUpgradeAnnouncement.mapping.successorSynchronizerId

              Either.cond(
                previouslyAnnouncedSuccessorPSId < upgradeAnnouncement.successorSynchronizerId,
                (),
                InvalidSynchronizerSuccessor.Reject.conflictWithPreviousAnnouncement(
                  successorSynchronizerId = upgradeAnnouncement.successorSynchronizerId,
                  previouslyAnnouncedSuccessor = previouslyAnnouncedSuccessorPSId,
                ),
              )
            }
        }
      }

    EitherT(resF)
  }

  private def checkSigningThresholdCanBeReached(
      threshold: PositiveInt,
      keys: NonEmpty[Set[SigningPublicKey]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyManagerError](
      keys.sizeIs >= threshold.value,
      TopologyManagerError.SigningThresholdCannotBeReached.Reject(threshold, keys.size),
    )

  private def checkPartyToParticipantIsNotDangerous(
      partyId: PartyId,
      threshold: PositiveInt,
      nextParticipants: Seq[HostingParticipant],
      signingKeysWithThresholdO: Option[SigningKeysWithThreshold],
      forceChanges: ForceFlags,
      operation: TopologyChangeOp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val currentThresholdAndHostingParticipants
        : FutureUnlessShutdown[(Option[PositiveInt], Seq[HostingParticipant])] =
      store
        .findPositiveTransactions(
          asOf = CantonTimestamp.MaxValue,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(PartyToParticipant.code),
          filterUid = Some(NonEmpty(Seq, partyId.uid)),
          filterNamespace = None,
        )
        .map {
          _.collectOfMapping[PartyToParticipant].collectLatestByUniqueKey.toTopologyState
            .collectFirst {
              case PartyToParticipant(_, currentThreshold, currentHostingParticipants, _) =>
                Some(currentThreshold) -> currentHostingParticipants
            }
            .getOrElse(None -> Nil)
        }

    for {
      _ <- signingKeysWithThresholdO.traverse_ { case SigningKeysWithThreshold(keys, threshold) =>
        checkSigningThresholdCanBeReached(threshold, keys)
      }
      _ <- checkConfirmingThresholdIsNotAboveNumberOfHostingNodes(
        threshold,
        nextParticipants.length,
        forceChanges: ForceFlags,
      )

      currentThresholdAndHostingParticipants <- EitherT.right(
        currentThresholdAndHostingParticipants
      )
      currentThresholdO = currentThresholdAndHostingParticipants._1
      currentHostingParticipants = currentThresholdAndHostingParticipants._2

      removed = operation match {
        case TopologyChangeOp.Replace =>
          currentHostingParticipants.map(_.participantId.uid).toSet -- nextParticipants.map(
            _.participantId.uid
          )
        case TopologyChangeOp.Remove => currentHostingParticipants.map(_.participantId.uid).toSet

      }

      isAlreadyPureObserver = currentHostingParticipants.forall(_.permission == Observation)
      isBecomingPureObserver = nextParticipants.forall(_.permission == Observation)
      isTransitionToPureObserver = !isAlreadyPureObserver && isBecomingPureObserver

      _ <-
        if (removed.contains(nodeId)) {
          checkCannotDisablePartyWithActiveContracts(partyId, forceChanges: ForceFlags)
        } else EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())

      _ <-
        if (
          isTransitionToPureObserver && currentHostingParticipants.exists(
            _.participantId.uid == nodeId
          )
        ) {
          checkInsufficientParticipantPermissionForSignatoryParty(partyId, forceChanges: ForceFlags)
        } else EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())

      (nextThreshold, nexHostingParticipants) = operation match {
        case TopologyChangeOp.Replace => Some(threshold) -> nextParticipants
        case TopologyChangeOp.Remove => None -> Nil
      }

      _ <- currentThresholdO.traverse_(currentThreshold =>
        checkInsufficientSignatoryAssigningParticipantsForParty(
          partyId,
          currentThreshold,
          nextThreshold,
          nexHostingParticipants,
          forceChanges,
        )
      )

    } yield ()
  }

  /** notify observers about new transactions about to be stored */
  private def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown
      .sequence(
        observers
          .get()
          .map(_.addedNewTransactions(timestamp, transactions))
      )
      .map(_ => ())

  override protected def onClosed(): Unit = LifeCycle.close(store, sequentialQueue)(logger)

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

  /** Assigns the appropriate key usage for a given set of keys based on the current topology
    * request and necessary authorizations. In most cases, the request is expected to be signed with
    * Namespace keys. However, for requests like OwnerToKeyMapping or PartyToKeyMapping, keys must
    * be able to prove their ownership. For these requests we also accept namespace as a valid usage
    * when verifying a signature, as this ensures backwards compatibility (e.g. older topology
    * states might have mistakenly added a namespace key to this mapping). By enforcing only
    * ProofOfOwnership on the sign path, we ensure that this new restriction applies to any newly
    * added key, preventing the addition of namespace-only keys to these requests.
    *
    * @param mapping
    *   The current topology request
    * @param signingKeys
    *   A non-empty set of signing key fingerprints for which a usage will be assigned.
    * @param forSigning
    *   A flag indicating whether usages are being assigned for signing or signature verification.
    * @return
    *   A map where each key is associated with its expected usage.
    */
  def assignExpectedUsageToKeys(
      mapping: TopologyMapping,
      signingKeys: NonEmpty[Set[Fingerprint]],
      forSigning: Boolean,
  ): NonEmpty[Map[Fingerprint, NonEmpty[Set[SigningKeyUsage]]]] = {

    def onlyNamespaceAuth(auth: RequiredAuth): Boolean = auth.referenced.isEmpty

    // True if the mapping must be signed only by a namespace key but not extra keys
    val strictNamespaceAuth = onlyNamespaceAuth(mapping.requiredAuth(None))

    mapping match {
      // Keys defined in Keymappings must prove the ownership of the mapped keys
      case keyMapping: KeyMapping if !strictNamespaceAuth =>
        val mappedKeyIds = keyMapping.mappedKeys.map[Fingerprint](_.id)
        val namespaceKeyForSelfAuthorization =
          keyMapping.namespaceKeyForSelfAuthorization.map(_.fingerprint)
        signingKeys.map {
          case keyId if namespaceKeyForSelfAuthorization.contains(keyId) =>
            // the key specified for self-authorization must be a namespace key
            keyId -> SigningKeyUsage.NamespaceOnly
          case keyId if mappedKeyIds.contains(keyId) =>
            if (forSigning)
              // the mapped keys need to prove ownership
              keyId -> SigningKeyUsage.ProofOfOwnershipOnly
            else
              // We need to verify proof of ownership for the mapped keys. The expected key usage is
              // ProofOfOwnership, analog what is used for signing. However, for backwards compatibility we
              // also allow keys with NamespaceOnly to have proven ownership in historical topology states.
              keyId -> SigningKeyUsage.NamespaceOrProofOfOwnership
          case keyId =>
            // all other keys must be namespace delegation keys
            keyId -> SigningKeyUsage.NamespaceOnly
        }.toMap

      case _ =>
        // If strict namespace authorization is true, either a namespace key or an identity delegation can sign
        signingKeys.map(_ -> SigningKeyUsage.NamespaceOnly).toMap

    }
  }

  def checkBounds(
      parameters: DynamicSynchronizerParameters
  )(implicit errorLoggingContext: ErrorLoggingContext): Either[TopologyManagerError, Unit] = {
    def check(
        proj: DynamicSynchronizerParameters => NonNegativeFiniteDuration,
        name: String,
        bounds: (NonNegativeFiniteDuration, NonNegativeFiniteDuration),
    ): Either[TopologyManagerError, Unit] = {
      val value = proj(parameters)

      val (min, max) = bounds

      for {
        _ <- Either.cond(value >= min, (), ValueOutOfBounds.Error(value, name, min, max))
        _ <- Either.cond(value <= max, (), ValueOutOfBounds.Error(value, name, min, max))
      } yield ()
    }

    for {
      _ <- check(
        _.confirmationResponseTimeout,
        "confirmation response timeout",
        DynamicSynchronizerParameters.confirmationResponseTimeoutBounds,
      )
      _ <- check(
        _.mediatorReactionTimeout,
        "mediator reaction timeout",
        DynamicSynchronizerParameters.mediatorReactionTimeoutBounds,
      )
    } yield ()
  }
}
