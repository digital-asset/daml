// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.CantonTimestamp.now
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.IncreaseOfLedgerTimeRecordTimeTolerance
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  IncomingTopologyTransactionAuthorizationValidator,
  SequencedTime,
  SnapshotAuthorizationValidator,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

abstract class TopologyManager[E <: CantonError](
    val clock: Clock,
    val crypto: Crypto,
    protected val store: TopologyStore[TopologyStoreId],
    timeouts: ProcessingTimeout,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends TopologyManagerStatus
    with NamedLogging
    with FlagCloseableAsync {

  protected val validator =
    new IncomingTopologyTransactionAuthorizationValidator(
      crypto.pureCrypto,
      store,
      None,
      loggerFactory.append("role", "manager"),
    )

  /** returns the current queue size (how many changes are being processed) */
  override def queueSize: Int = sequentialQueue.queueSize

  protected def checkTransactionNotAddedBefore(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] = {
    val ret = store
      .exists(transaction)
      .map(x =>
        Either.cond(
          !x,
          (),
          TopologyManagerError.DuplicateTransaction
            .Failure(transaction.transaction, transaction.key.fingerprint),
        )
      )
    EitherT(ret)
  }

  protected def checkRemovalRefersToExisingTx(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] =
    if (
      transaction.operation == TopologyChangeOp.Add || transaction.operation == TopologyChangeOp.Replace
    )
      EitherT.rightT(())
    else {
      for {
        active <- EitherT.right(
          store.findPositiveTransactionsForMapping(transaction.transaction.element.mapping)
        )
        filtered = active.find(sit => sit.transaction.element == transaction.transaction.element)
        _ <- EitherT.cond[Future](
          filtered.nonEmpty,
          (),
          TopologyManagerError.NoCorrespondingActiveTxToRevoke.Element(
            transaction.transaction.element
          ): TopologyManagerError,
        )
      } yield ()
    }

  protected def keyRevocationIsNotDangerous(
      owner: Member,
      key: PublicKey,
      elementId: TopologyElementId,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] = {
    lazy val removingLastKeyMustBeForcedError: TopologyManagerError =
      TopologyManagerError.RemovingLastKeyMustBeForced.Failure(key.fingerprint, key.purpose)

    for {
      txs <- EitherT.right(
        store.findPositiveTransactions(
          // Use the max timestamp so that we get the head state
          CantonTimestamp.MaxValue,
          asOfInclusive = true,
          includeSecondary = false,
          types = Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
          filterUid = Some(Seq(owner.uid)),
          filterNamespace = None,
        )
      )
      remaining = txs.toIdentityState.collect {
        case TopologyStateUpdateElement(id, OwnerToKeyMapping(`owner`, remainingKey))
            if id != elementId && key.purpose == remainingKey.purpose =>
          key
      }
      _ <-
        if (force && remaining.isEmpty) {
          logger.info(s"Transaction will forcefully remove last ${key.purpose} of $owner")
          EitherT.rightT[Future, TopologyManagerError](())
        } else EitherT.cond[Future](remaining.nonEmpty, (), removingLastKeyMustBeForcedError)
    } yield ()
  }

  protected def keyRevocationDelegationIsNotDangerous(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      namespace: Namespace,
      targetKey: SigningPublicKey,
      force: Boolean,
      removeFromCache: (
          SnapshotAuthorizationValidator,
          StoredTopologyTransactions[TopologyChangeOp],
      ) => EitherT[FutureUnlessShutdown, TopologyManagerError, Unit],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {

    lazy val unauthorizedTransaction: TopologyManagerError =
      TopologyManagerError.UnauthorizedTransaction.Failure()

    lazy val removingKeyWithDanglingTransactionsMustBeForcedError: TopologyManagerError =
      TopologyManagerError.RemovingKeyWithDanglingTransactionsMustBeForced
        .Failure(targetKey.fingerprint, targetKey.purpose)

    val validatorSnap =
      new SnapshotAuthorizationValidator(now(), store, timeouts, loggerFactory, futureSupervisor)

    for {
      // step1: check if transaction is authorized
      authorized <- EitherT.right[TopologyManagerError](validatorSnap.authorizedBy(transaction))
      _ <-
        // not authorized
        if (authorized.isEmpty)
          EitherT.leftT[FutureUnlessShutdown, Unit](unauthorizedTransaction: TopologyManagerError)
        // authorized
        else if (!force) {
          for {
            // step2: find transaction that is going to be removed
            storedTxsToRemove <- EitherT
              .right[TopologyManagerError](
                store.findStoredNoSignature(transaction.transaction.reverse)
              )
              .mapK(FutureUnlessShutdown.outcomeK)

            // step3: remove namespace delegation transaction from cache store
            _ <- storedTxsToRemove.parTraverse { storedTxToRemove =>
              {
                val wrapStoredTx =
                  new StoredTopologyTransactions[TopologyChangeOp](Seq(storedTxToRemove))
                removeFromCache(validatorSnap, wrapStoredTx)
              }
            }

            // step4: retrieve all transactions (possibly related with this namespace)
            // TODO(i9809): this is risky for a big number of parties (i.e. 1M)
            txs <- EitherT
              .right(
                store.findPositiveTransactions(
                  CantonTimestamp.MaxValue,
                  asOfInclusive = true,
                  includeSecondary = true,
                  types = DomainTopologyTransactionType.all,
                  filterUid = None,
                  filterNamespace = Some(Seq(namespace)),
                )
              )
              .mapK(FutureUnlessShutdown.outcomeK)

            // step5: check if these transactions are still valid
            _ <- txs.combine.result.parTraverse { txToCheck =>
              EitherT(
                validatorSnap
                  .authorizedBy(txToCheck.transaction)
                  .map(res =>
                    Either
                      .cond(res.nonEmpty, (), removingKeyWithDanglingTransactionsMustBeForcedError)
                  )
              )
            }
          } yield ()
        } else
          EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())
    } yield ()
  }

  protected def transactionIsNotDangerous(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    if (transaction.transaction.op == TopologyChangeOp.Add)
      EitherT.rightT(())
    else {
      transaction.transaction.element.mapping match {
        case OwnerToKeyMapping(owner, key) =>
          keyRevocationIsNotDangerous(owner, key, transaction.transaction.element.id, force)
            .mapK(FutureUnlessShutdown.outcomeK)
        case NamespaceDelegation(namespace, targetKey, _) =>
          keyRevocationDelegationIsNotDangerous(
            transaction,
            namespace,
            targetKey,
            force,
            { (validatorSnap, transaction) =>
              EitherT.right[TopologyManagerError](
                validatorSnap
                  .removeNamespaceDelegationFromCache(namespace, transaction)
              )
            },
          )
        case IdentifierDelegation(uniqueKey, targetKey) =>
          keyRevocationDelegationIsNotDangerous(
            transaction,
            uniqueKey.namespace,
            targetKey,
            force,
            { (validatorSnap, transaction) =>
              EitherT.right[TopologyManagerError](
                validatorSnap
                  .removeIdentifierDelegationFromCache(uniqueKey, transaction)
              )
            },
          )
        case DomainParametersChange(_, newDomainParameters) if !force =>
          checkLedgerTimeRecordTimeToleranceNotIncreasing(newDomainParameters).mapK(
            FutureUnlessShutdown.outcomeK
          )
        case _ => EitherT.rightT(())
      }
    }

  def signedMappingAlreadyExists(
      mapping: TopologyMapping,
      signingKey: Fingerprint,
  )(implicit traceContext: TraceContext): Future[Boolean] =
    for {
      txs <- store.findPositiveTransactionsForMapping(mapping)
      mappings = txs.map(x => (x.transaction.element.mapping, x.key.fingerprint))
    } yield mappings.contains((mapping, signingKey))

  protected def checkMappingOfTxDoesNotExistYet(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      allowDuplicateMappings: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] =
    if (allowDuplicateMappings || transaction.transaction.op != TopologyChangeOp.Add) {
      EitherT.rightT(())
    } else {
      (for {
        exists <- EitherT.right(
          signedMappingAlreadyExists(
            transaction.transaction.element.mapping,
            transaction.key.fingerprint,
          )
        )
        _ <- EitherT.cond[Future](
          !exists,
          (),
          TopologyManagerError.MappingAlreadyExists.Failure(
            transaction.transaction.element,
            transaction.key.fingerprint,
          ): TopologyManagerError,
        )
      } yield ()): EitherT[Future, TopologyManagerError, Unit]
    }

  private def checkLedgerTimeRecordTimeToleranceNotIncreasing(
      newDomainParameters: DynamicDomainParameters
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] = {
    // See i9028 for a detailed design.

    EitherT(for {
      headTransactions <- store.headTransactions
    } yield {
      val domainParameters = headTransactions.toTopologyState
        .collectFirst { case DomainGovernanceElement(DomainParametersChange(_, domainParameters)) =>
          domainParameters
        }
        .getOrElse(DynamicDomainParameters.initialValues(clock, protocolVersion))
      Either.cond(
        domainParameters.ledgerTimeRecordTimeTolerance >= newDomainParameters.ledgerTimeRecordTimeTolerance,
        (),
        IncreaseOfLedgerTimeRecordTimeTolerance.TemporarilyInsecure(
          domainParameters.ledgerTimeRecordTimeTolerance,
          newDomainParameters.ledgerTimeRecordTimeTolerance,
        ),
      )
    })
  }

  protected def checkNewTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, Unit]

  protected def build[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: Option[Fingerprint],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, SignedTopologyTransaction[Op]] = {
    for {
      // find signing key
      key <- signingKey match {
        case Some(key) => EitherT.rightT[Future, E](key)
        case None => signingKeyForTransactionF(transaction)
      }
      // fetch public key
      pubkey <- crypto.cryptoPublicStore
        .signingKey(key)
        .leftMap(x => wrapError(TopologyManagerError.InternalError.CryptoPublicError(x)))
        .subflatMap(_.toRight(wrapError(TopologyManagerError.PublicKeyNotInStore.Failure(key))))
      // create signed transaction
      signed <- SignedTopologyTransaction
        .create(
          transaction,
          pubkey,
          crypto.pureCrypto,
          crypto.privateCrypto,
          protocolVersion,
        )
        .leftMap {
          case SigningError.UnknownSigningKey(keyId) =>
            wrapError(TopologyManagerError.SecretKeyNotInStore.Failure(keyId))
          case err => wrapError(TopologyManagerError.InternalError.TopologySigningError(err))
        }
    } yield signed
  }

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param transaction the transaction to be signed and added
    * @param signingKey  the key which should be used to sign
    * @param protocolVersion the protocol version corresponding to the transaction
    * @param force       force dangerous operations, such as removing the last signing key of a participant
    * @param replaceExisting if true and the transaction op is add, then we'll replace existing active mappings before adding the new
    * @return            the domain state (initialized or not initialized) or an error code of why the addition failed
    */
  def authorize[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: Option[Fingerprint],
      protocolVersion: ProtocolVersion,
      force: Boolean = false,
      replaceExisting: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, E, SignedTopologyTransaction[Op]] = {
    sequentialQueue.executeEUS(
      {
        logger.debug(show"Attempting to authorize ${transaction.element.mapping} with $signingKey")
        for {
          signed <- build(transaction, signingKey, protocolVersion).mapK(
            FutureUnlessShutdown.outcomeK
          )
          _ <- process(signed, force, replaceExisting, allowDuplicateMappings = false)
        } yield signed
      },
      "authorize transaction",
    )
  }

  protected def signingKeyForTransactionF(
      transaction: TopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, E, Fingerprint] = {
    for {
      // need to execute signing key finding sequentially, as the validator is expecting incremental in-memory updates
      // to the "autohrization graph"
      keys <- EitherT.right(
        validator.getValidSigningKeysForMapping(clock.uniqueTime(), transaction.element.mapping)
      )
      fingerprint <- findSigningKey(keys).leftMap(wrapError)
    } yield fingerprint
  }

  private def findSigningKey(
      keys: Seq[Fingerprint]
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Fingerprint] =
    keys.reverse.toList
      .parFilterA(fingerprint =>
        crypto.cryptoPrivateStore
          .existsSigningKey(fingerprint)
      )
      .map(x => x.headOption)
      .leftMap[TopologyManagerError](x => TopologyManagerError.InternalError.CryptoPrivateError(x))
      .subflatMap(_.toRight(TopologyManagerError.NoAppropriateSigningKeyInStore.Failure(keys)))

  def add(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean = false,
      replaceExisting: Boolean = false,
      allowDuplicateMappings: Boolean = false,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, E, Unit] = {
    // Ensure sequential execution of `process`: When processing signed topology transactions, we test whether they can be
    // added incrementally to the existing state. Therefore, we need to sequence
    // (testing + adding) and ensure that we don't concurrently insert these
    // transactions.
    sequentialQueue.executeEUS(
      process(transaction, force, replaceExisting, allowDuplicateMappings),
      "add transaction",
    )
  }

  protected val sequentialQueue = new SimpleExecutionQueue(
    "topology-manager-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  /** sequential(!) processing of topology transactions
    *
    * @param force force a dangerous change (such as revoking the last key)
    * @param allowDuplicateMappings whether to reject a transaction if a similar transaction leading to the same result already exists
    */
  protected def process[Op <: TopologyChangeOp](
      transaction: SignedTopologyTransaction[Op],
      force: Boolean,
      replaceExisting: Boolean,
      allowDuplicateMappings: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, E, Unit] = {
    def checkValidationResult(
        validated: Seq[ValidatedTopologyTransaction]
    ): EitherT[Future, E, Unit] = {
      EitherT.fromEither((validated.find(_.rejectionReason.nonEmpty) match {
        case Some(
              ValidatedTopologyTransaction(
                `transaction`,
                Some(rejection),
              )
            ) =>
          Left(rejection.toTopologyManagerError)
        case Some(tx: ValidatedTopologyTransaction) =>
          Left(TopologyManagerError.InternalError.ReplaceExistingFailed(tx))
        case None => Right(())
      }).leftMap(wrapError))
    }

    def addOneByOne(
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
    ): EitherT[Future, E, Unit] = {
      MonadUtil.sequentialTraverse_(transactions) { tx =>
        val now = clock.uniqueTime()
        preNotifyObservers(Seq(tx))
        logger.info(
          show"Applied topology transaction ${tx.transaction.op} ${tx.transaction.element.mapping} at $now"
        )
        for {
          _ <- EitherT.right(
            store.append(
              SequencedTime(now),
              EffectiveTime(now),
              Seq(ValidatedTopologyTransaction.valid(tx)),
            )
          ): EitherT[Future, E, Unit]
          _ <- EitherT.right(notifyObservers(now, Seq(tx)))
        } yield ()
      }
    }

    val isUniquenessRequired = transaction.operation match {
      case TopologyChangeOp.Replace => false
      case _ => true
    }

    val now = clock.uniqueTime()
    val ret = for {
      // uniqueness check on store: ensure that transaction hasn't been added before
      _ <-
        if (isUniquenessRequired)
          checkTransactionNotAddedBefore(transaction)
            .leftMap(wrapError)
            .mapK(FutureUnlessShutdown.outcomeK)
        else EitherT.pure[FutureUnlessShutdown, E](())
      _ <- checkRemovalRefersToExisingTx(transaction)
        .leftMap(wrapError)
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- checkMappingOfTxDoesNotExistYet(transaction, allowDuplicateMappings)
        .leftMap(wrapError)
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- transactionIsNotDangerous(transaction, force).leftMap(wrapError)
      _ <- checkNewTransaction(transaction, force).mapK(
        FutureUnlessShutdown.outcomeK
      ) // domain / participant specific checks
      deactivateExisting <- removeExistingTransactions(transaction, replaceExisting).mapK(
        FutureUnlessShutdown.outcomeK
      )
      updateTx = transaction +: deactivateExisting
      res <- EitherT
        .right(validator.validateAndUpdateHeadAuthState(now, updateTx))
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- checkValidationResult(res._2).mapK(FutureUnlessShutdown.outcomeK)
      // TODO(i1251) batch adding once we overhaul the domain identity dispatcher (right now, adding multiple tx with same ts doesn't work)
      _ <- addOneByOne(updateTx).mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

    ret.leftMap { err =>
      // if there was an intermittent failure, just reset the auth validator (will reload the state)
      validator.reset()
      err
    }
  }

  protected def removeExistingTransactions(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      replaceExisting: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, Seq[SignedTopologyTransaction[TopologyChangeOp]]] =
    if (!replaceExisting || transaction.operation == TopologyChangeOp.Remove) {
      EitherT.rightT(Seq())
    } else {
      val (nsFilter, uidFilter) = transaction.uniquePath.maybeUid match {
        case Some(uid) => (None, Some(Seq(uid)))
        case None => (Some(Seq(transaction.uniquePath.namespace)), None)
      }

      for {
        rawTxs <- EitherT.right(
          store.findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            includeSecondary = false,
            types = Seq(transaction.uniquePath.dbType),
            filterUid = uidFilter,
            filterNamespace = nsFilter,
          )
        )
        reverse <- MonadUtil.sequentialTraverse(
          rawTxs.adds.toDomainTopologyTransactions
            .filter(
              _.transaction.element.mapping.isReplacedBy(
                transaction.transaction.element.mapping
              )
            )
        )(x =>
          build(
            x.transaction.reverse,
            None,
            protocolVersion,
          )
        )
      } yield reverse
    }

  protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit]

  protected def preNotifyObservers(transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]])(
      implicit traceContext: TraceContext
  ): Unit = {}

  protected def wrapError(error: TopologyManagerError)(implicit traceContext: TraceContext): E

  def genTransaction(
      op: TopologyChangeOp,
      mapping: TopologyMapping,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, TopologyTransaction[TopologyChangeOp]] = {
    import TopologyChangeOp.*
    (op, mapping) match {
      case (Add, mapping: TopologyStateUpdateMapping) =>
        EitherT.rightT(TopologyStateUpdate.createAdd(mapping, protocolVersion))

      case (Remove, mapping: TopologyStateUpdateMapping) =>
        for {
          tx <- EitherT(
            store
              .findPositiveTransactionsForMapping(mapping)
              .map(
                _.headOption.toRight[TopologyManagerError](
                  TopologyManagerError.NoCorrespondingActiveTxToRevoke.Mapping(mapping)
                )
              )
          )
        } yield tx.transaction.reverse

      case (Replace, mapping: DomainGovernanceMapping) =>
        EitherT.pure(DomainGovernanceTransaction(mapping, protocolVersion))

      case (op, mapping) =>
        EitherT.fromEither(
          Left(TopologyManagerError.InternalError.IncompatibleOpMapping(op, mapping))
        )
    }
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    Seq(
      SyncCloseable("topology-manager-store", store.close()),
      SyncCloseable("topology-manager-sequential-queue", sequentialQueue.close()),
    )
  }

}
