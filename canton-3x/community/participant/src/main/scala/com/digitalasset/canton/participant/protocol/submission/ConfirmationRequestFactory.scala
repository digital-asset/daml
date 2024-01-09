// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.LoggingConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory.*
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.{
  ContractConsistencyChecker,
  ExtractUsedContractsFromRootViews,
}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Factory class for creating confirmation requests from Daml-LF transactions.
  *
  * @param transactionTreeFactory used to create the payload
  * @param seedGenerator used to derive the transaction seed
  * @param parallel to flag if view processing is done in parallel or sequentially. Intended to be set only during tests
  *                 to enforce determinism, otherwise it is always set to true.
  */
class ConfirmationRequestFactory(
    submitterNode: ParticipantId,
    loggingConfig: LoggingConfig,
    val loggerFactory: NamedLoggerFactory,
    parallel: Boolean = true,
)(val transactionTreeFactory: TransactionTreeFactory, seedGenerator: SeedGenerator)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  /** Creates a confirmation request from a wellformed transaction.
    *
    * @param cryptoSnapshot used to determine participants of parties and for signing and encryption
    * @return the confirmation request and the transaction root hash (aka transaction id) or an error. See the
    *         documentation of [[com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory.ConfirmationRequestCreationError]]
    *         for more information on error cases.
    */
  def createConfirmationRequest(
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      confirmationPolicy: ConfirmationPolicy,
      submitterInfo: SubmitterInfo,
      workflowId: Option[WorkflowId],
      keyResolver: LfKeyResolver,
      mediator: MediatorRef,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      contractInstanceOfId: SerializableContractOfId,
      optKeySeed: Option[SecureRandomness],
      maxSequencingTime: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ConfirmationRequestCreationError, ConfirmationRequest] = {
    val transactionUuid = seedGenerator.generateUuid()
    val ledgerTime = wfTransaction.metadata.ledgerTime

    val keySeed = optKeySeed.getOrElse(createDefaultSeed(cryptoSnapshot.pureCrypto))

    for {
      _ <- assertSubmittersNodeAuthorization(submitterInfo.actAs, cryptoSnapshot.ipsSnapshot)

      // Starting with Daml 1.6.0, the daml engine performs authorization validation.

      transactionSeed = seedGenerator.generateSaltSeed()

      transactionTree <- transactionTreeFactory
        .createTransactionTree(
          wfTransaction,
          submitterInfo,
          confirmationPolicy,
          workflowId,
          mediator,
          transactionSeed,
          transactionUuid,
          cryptoSnapshot.ipsSnapshot,
          contractInstanceOfId,
          keyResolver,
          maxSequencingTime,
          validatePackageVettings = true,
        )
        .leftMap(TransactionTreeFactoryError)

      rootViews = transactionTree.rootViews.unblindedElements.toList
      inputContracts = ExtractUsedContractsFromRootViews(rootViews)
      _ <- EitherT.fromEither[Future](
        ContractConsistencyChecker
          .assertInputContractsInPast(inputContracts, ledgerTime)
          .leftMap(errs => ContractConsistencyError(errs))
      )

      confirmationRequest <- createConfirmationRequest(
        transactionTree,
        cryptoSnapshot,
        sessionKeyStore,
        keySeed,
        protocolVersion,
      )
    } yield confirmationRequest
  }

  def createDefaultSeed(pureCrypto: CryptoPureApi): SecureRandomness = {
    val randomnessLength = EncryptedViewMessage.computeRandomnessLength(pureCrypto)
    pureCrypto.generateSecureRandomness(randomnessLength)
  }

  def createConfirmationRequest(
      transactionTree: GenTransactionTree,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      keySeed: SecureRandomness,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ConfirmationRequestCreationError, ConfirmationRequest] = for {
    transactionViewEnvelopes <- createTransactionViewEnvelopes(
      transactionTree,
      cryptoSnapshot,
      sessionKeyStore,
      keySeed,
      protocolVersion,
    )
  } yield {
    if (loggingConfig.eventDetails) {
      logger.debug(
        s"Transaction tree is ${loggingConfig.api.printer.printAdHoc(transactionTree)}"
      )
    }
    ConfirmationRequest(
      InformeeMessage(transactionTree.tryFullInformeeTree(protocolVersion))(protocolVersion),
      transactionViewEnvelopes,
      protocolVersion,
    )
  }

  private def assertSubmittersNodeAuthorization(
      submitters: List[LfPartyId],
      identities: TopologySnapshot,
  ): EitherT[Future, ParticipantAuthorizationError, Unit] = {

    def assertSubmitterNodeAuthorization(submitter: LfPartyId) = {
      for {
        relationship <- EitherT(
          identities
            .hostedOn(submitter, submitterNode)
            .map(
              _.toRight(
                ParticipantAuthorizationError(
                  s"$submitterNode does not host $submitter or is not active."
                )
              )
            )
        )
        _ <- EitherT.cond[Future](
          relationship.permission == Submission,
          (),
          ParticipantAuthorizationError(
            s"$submitterNode is not authorized to submit transactions for $submitter."
          ),
        )
      } yield ()
    }
    submitters.parTraverse(assertSubmitterNodeAuthorization).map(_ => ())
  }

  private def createTransactionViewEnvelopes(
      transactionTree: GenTransactionTree,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      keySeed: SecureRandomness,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): EitherT[Future, ConfirmationRequestCreationError, List[
    OpenEnvelope[TransactionViewMessage]
  ]] = {
    val pureCrypto = cryptoSnapshot.pureCrypto

    def createOpenEnvelopeWithTransaction(
        vt: LightTransactionViewTree,
        seed: SecureRandomness,
        witnesses: Witnesses,
    ): EitherT[Future, ConfirmationRequestCreationError, OpenEnvelope[TransactionViewMessage]] =
      for {
        viewMessage <- EncryptedViewMessageFactory
          .create(TransactionViewType)(
            vt,
            cryptoSnapshot,
            sessionKeyStore,
            protocolVersion,
            Some(seed),
          )
          .leftMap(EncryptedViewMessageCreationError)
        recipients <- witnesses
          .toRecipients(cryptoSnapshot.ipsSnapshot)
          .leftMap[ConfirmationRequestCreationError](e => RecipientsCreationError(e.message))
      } yield OpenEnvelope(viewMessage, recipients)(protocolVersion)

    for {
      lightTreesWithMetadata <- EitherT.fromEither[Future](
        transactionTree
          .allLightTransactionViewTreesWithWitnessesAndSeeds(keySeed, pureCrypto)
          .leftMap(KeySeedError)
      )

      res <-
        if (parallel)
          lightTreesWithMetadata.toList.parTraverse { case (vt, witnesses, seed) =>
            createOpenEnvelopeWithTransaction(vt, seed, witnesses)
          }
        else
          MonadUtil.sequentialTraverse(lightTreesWithMetadata) { case (vt, witnesses, seed) =>
            createOpenEnvelopeWithTransaction(vt, seed, witnesses)
          }
    } yield res.toList
  }
}

object ConfirmationRequestFactory {
  def apply(submitterNode: ParticipantId, domainId: DomainId, protocolVersion: ProtocolVersion)(
      cryptoOps: HashOps with HmacOps,
      seedGenerator: SeedGenerator,
      loggingConfig: LoggingConfig,
      uniqueContractKeys: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ConfirmationRequestFactory = {

    val transactionTreeFactory =
      TransactionTreeFactoryImpl(
        submitterNode,
        domainId,
        protocolVersion,
        cryptoOps,
        uniqueContractKeys,
        loggerFactory,
      )

    new ConfirmationRequestFactory(submitterNode, loggingConfig, loggerFactory)(
      transactionTreeFactory,
      seedGenerator,
    )
  }

  /** Superclass for all errors that may arise during the creation of a confirmation request.
    */
  sealed trait ConfirmationRequestCreationError
      extends Product
      with Serializable
      with PrettyPrinting

  /** Indicates that the submitterNode is not allowed to represent the submitter or to submit requests.
    */
  final case class ParticipantAuthorizationError(message: String)
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[ParticipantAuthorizationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the given transaction is malformed in some way, e.g., it has cycles.
    */
  final case class MalformedLfTransaction(message: String)
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[MalformedLfTransaction] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the submitter could not be mapped to a party.
    */
  final case class MalformedSubmitter(message: String) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[MalformedSubmitter] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the given transaction is contract-inconsistent.
    */
  final case class ContractConsistencyError(errors: Seq[ReferenceToFutureContractError])
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[ContractConsistencyError] = prettyOfClass(unnamedParam(_.errors))
  }

  /** Indicates that the given transaction is contract key-inconsistent. */
  final case class ContractKeyConsistencyError(key: LfGlobalKey)
      extends ConfirmationRequestCreationError
      with PrettyPrinting {
    override def pretty: Pretty[ContractKeyConsistencyError] =
      prettyOfClass(
        param("key", _.key)
      )
  }

  /** Indicates that the given transaction yields a duplicate for a contract ID. */
  final case class ContractIdDuplicateError(contractId: LfContractId)
      extends ConfirmationRequestCreationError
      with PrettyPrinting {
    override def pretty: Pretty[ContractIdDuplicateError] =
      prettyOfClass(
        param("contractId", _.contractId)
      )
  }

  /** Indicates that the given transaction yields a duplicate for a key. */
  final case class ContractKeyDuplicateError(key: LfGlobalKey)
      extends ConfirmationRequestCreationError
      with PrettyPrinting {
    override def pretty: Pretty[ContractKeyDuplicateError] =
      prettyOfClass(
        param("key", _.key)
      )
  }

  /** Indicates that the encrypted view message could not be created. */
  final case class EncryptedViewMessageCreationError(
      error: EncryptedViewMessageFactory.EncryptedViewMessageCreationError
  ) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[EncryptedViewMessageCreationError] = prettyOfParam(_.error)
  }

  /** Indicates that the transaction could not be converted to a transaction tree.
    * @see TransactionTreeFactory.TransactionTreeConversionError for more information.
    */
  final case class TransactionTreeFactoryError(cause: TransactionTreeConversionError)
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[TransactionTreeFactoryError] = prettyOfParam(_.cause)
  }

  final case class RecipientsCreationError(message: String)
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[RecipientsCreationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class KeySeedError(cause: HkdfError) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[KeySeedError] = prettyOfParam(_.cause)
  }
}
