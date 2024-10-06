// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.LoggingConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.GenTransactionTree.ViewWithWitnessesAndRecipients
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.*
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
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Factory class for creating transaction confirmation requests from Daml-LF transactions.
  *
  * @param transactionTreeFactory used to create the payload
  * @param seedGenerator used to derive the transaction seed
  * @param parallel to flag if view processing is done in parallel or sequentially. Intended to be set only during tests
  *                 to enforce determinism, otherwise it is always set to true.
  */
class TransactionConfirmationRequestFactory(
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
    *         documentation of [[com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.TransactionConfirmationRequestCreationError]]
    *         for more information on error cases.
    */
  def createConfirmationRequest(
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      workflowId: Option[WorkflowId],
      keyResolver: LfKeyResolver,
      mediator: MediatorGroupRecipient,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      contractInstanceOfId: SerializableContractOfId,
      maxSequencingTime: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionConfirmationRequestCreationError,
    TransactionConfirmationRequest,
  ] = {
    val transactionUuid = seedGenerator.generateUuid()
    val ledgerTime = wfTransaction.metadata.ledgerTime

    for {
      _ <- assertSubmittersNodeAuthorization(submitterInfo.actAs, cryptoSnapshot.ipsSnapshot).mapK(
        FutureUnlessShutdown.outcomeK
      )
      _ <- assertNonLocalPartyAuthorization(submitterInfo, cryptoSnapshot)

      // Starting with Daml 1.6.0, the daml engine performs authorization validation.

      transactionSeed = seedGenerator.generateSaltSeed()

      transactionTree <- transactionTreeFactory
        .createTransactionTree(
          wfTransaction,
          submitterInfo,
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
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        ContractConsistencyChecker
          .assertInputContractsInPast(inputContracts, ledgerTime)
          .leftMap(errs => ContractConsistencyError(errs))
      )

      confirmationRequest <- createConfirmationRequest(
        transactionTree,
        cryptoSnapshot,
        sessionKeyStore,
        protocolVersion,
      )
    } yield confirmationRequest
  }

  def createConfirmationRequest(
      transactionTree: GenTransactionTree,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionConfirmationRequestCreationError,
    TransactionConfirmationRequest,
  ] =
    for {
      transactionViewEnvelopes <- createTransactionViewEnvelopes(
        transactionTree,
        cryptoSnapshot,
        sessionKeyStore,
        protocolVersion,
      )
      submittingParticipantSignature <- cryptoSnapshot
        .sign(transactionTree.rootHash.unwrap)
        .leftMap[TransactionConfirmationRequestCreationError](TransactionSigningError)
    } yield {
      if (loggingConfig.eventDetails) {
        logger.debug(
          s"Transaction tree is ${loggingConfig.api.printer.printAdHoc(transactionTree)}"
        )
      }
      TransactionConfirmationRequest(
        InformeeMessage(
          transactionTree.tryFullInformeeTree(protocolVersion),
          submittingParticipantSignature,
        )(protocolVersion),
        transactionViewEnvelopes,
        protocolVersion,
      )
    }

  private def validatePartySignatures(
      submitterInfo: SubmitterInfo,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantAuthorizationError, Set[LfPartyId]] =
    submitterInfo.externallySignedTransaction match {
      case None => EitherT.rightT[FutureUnlessShutdown, ParticipantAuthorizationError](Set.empty)
      case Some(ExternallySignedTransaction(hash, partySignatures)) =>
        for {
          parties <- partySignatures
            .verifySignatures(hash, cryptoSnapshot)
            .leftMap(ParticipantAuthorizationError)
        } yield parties
    }

  private def assertNonLocalPartyAuthorization(
      submitterInfo: SubmitterInfo,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantAuthorizationError, Unit] =
    // TODO(i20746): This is the absolute minimum level of validation
    // We only check that provided signatures (if any) are valid
    // As we thread authorization checks through for external signing, we'll need to make sure that all "actAs" parties
    // are either hosted on this participant, or have provided a valid external signature.
    // _For now_, we still require all submitting parties to be hosted on this participant anyway (via assertSubmittersNodeAuthorization below),
    // so the external signatures are checked here ONLY, and are not used validated in phase 3.
    // We'll also need to check that parties with external signatures are hosted with confirmation rights somewhere, although
    // this may belong in the logic in AdmissibleDomains instead.
    validatePartySignatures(submitterInfo, cryptoSnapshot).void

  private def assertSubmittersNodeAuthorization(
      submitters: List[LfPartyId],
      identities: TopologySnapshot,
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantAuthorizationError, Unit] =
    EitherT(
      identities
        .hostedOn(submitters.toSet, submitterNode)
        .map { partyWithParticipants =>
          submitters
            .traverse(submitter =>
              partyWithParticipants
                .get(submitter)
                .toRight(
                  ParticipantAuthorizationError(
                    s"$submitterNode does not host $submitter or is not active."
                  )
                )
                .flatMap { relationship =>
                  Either.cond(
                    relationship.permission == Submission,
                    (),
                    ParticipantAuthorizationError(
                      s"$submitterNode is not authorized to submit transactions for $submitter."
                    ),
                  )
                }
            )
            .void
        }
    )

  private def createTransactionViewEnvelopes(
      transactionTree: GenTransactionTree,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionConfirmationRequestCreationError, List[
    OpenEnvelope[TransactionViewMessage]
  ]] = {
    val pureCrypto = cryptoSnapshot.pureCrypto

    def createOpenEnvelopeWithTransaction(
        vt: FullTransactionViewTree,
        viewsToKeyMap: Map[
          ViewHash,
          ViewKeyData,
        ],
        recipients: Recipients,
    ) = {
      val subviewsKeys = vt.subviewHashes
        .map(subviewHash => viewsToKeyMap(subviewHash).viewKeyRandomness)
      for {
        lvt <- LightTransactionViewTree
          .fromTransactionViewTree(vt, subviewsKeys, protocolVersion)
          .leftMap[TransactionConfirmationRequestCreationError](
            LightTransactionViewTreeCreationError
          )
          .toEitherT[FutureUnlessShutdown]
        ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(vt.viewHash)
        envelope <- EncryptedViewMessageFactory
          .create(TransactionViewType)(
            lvt,
            (viewKey, viewKeyMap),
            cryptoSnapshot,
            protocolVersion,
          )
          .leftMap[TransactionConfirmationRequestCreationError](EncryptedViewMessageCreationError)
          .map(viewMessage => OpenEnvelope(viewMessage, recipients)(protocolVersion))
      } yield envelope
    }

    for {
      lightTreesWithMetadata <- transactionTree
        .allTransactionViewTreesWithRecipients(cryptoSnapshot.ipsSnapshot)
        .leftMap[TransactionConfirmationRequestCreationError](e =>
          RecipientsCreationError(e.message)
        )
      viewsToKeyMap <- EncryptedViewMessageFactory
        .generateKeysFromRecipients(
          lightTreesWithMetadata.map { case ViewWithWitnessesAndRecipients(tvt, _, recipients) =>
            (ViewHashAndRecipients(tvt.viewHash, recipients), tvt.informees.toList)
          },
          parallel,
          pureCrypto,
          cryptoSnapshot,
          sessionKeyStore.convertStore,
          protocolVersion,
        )
        .leftMap[TransactionConfirmationRequestCreationError](e =>
          EncryptedViewMessageCreationError(e)
        )
      res <-
        if (parallel) {
          lightTreesWithMetadata.toList.parTraverse {
            case ViewWithWitnessesAndRecipients(tvt, _, recipients) =>
              createOpenEnvelopeWithTransaction(
                tvt,
                viewsToKeyMap,
                recipients,
              )
          }
        } else
          MonadUtil.sequentialTraverse(lightTreesWithMetadata) {
            case ViewWithWitnessesAndRecipients(tvt, _, recipients) =>
              createOpenEnvelopeWithTransaction(
                tvt,
                viewsToKeyMap,
                recipients,
              )
          }
    } yield res.toList
  }
}

object TransactionConfirmationRequestFactory {
  def apply(submitterNode: ParticipantId, domainId: DomainId, protocolVersion: ProtocolVersion)(
      cryptoOps: HashOps & HmacOps,
      seedGenerator: SeedGenerator,
      loggingConfig: LoggingConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): TransactionConfirmationRequestFactory = {

    val transactionTreeFactory =
      TransactionTreeFactoryImpl(
        submitterNode,
        domainId,
        protocolVersion,
        cryptoOps,
        loggerFactory,
      )

    new TransactionConfirmationRequestFactory(submitterNode, loggingConfig, loggerFactory)(
      transactionTreeFactory,
      seedGenerator,
    )
  }

  /** Superclass for all errors that may arise during the creation of a confirmation request.
    */
  sealed trait TransactionConfirmationRequestCreationError
      extends Product
      with Serializable
      with PrettyPrinting

  /** Indicates that the submitterNode is not allowed to represent the submitter or to submit requests.
    */
  final case class ParticipantAuthorizationError(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[ParticipantAuthorizationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the given transaction is malformed in some way, e.g., it has cycles.
    */
  final case class MalformedLfTransaction(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[MalformedLfTransaction] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the submitter could not be mapped to a party.
    */
  final case class MalformedSubmitter(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[MalformedSubmitter] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the given transaction is contract-inconsistent.
    */
  final case class ContractConsistencyError(errors: Seq[ReferenceToFutureContractError])
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[ContractConsistencyError] = prettyOfClass(
      unnamedParam(_.errors)
    )
  }

  /** Indicates that the encrypted view message could not be created. */
  final case class EncryptedViewMessageCreationError(
      error: EncryptedViewMessageFactory.EncryptedViewMessageCreationError
  ) extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[EncryptedViewMessageCreationError] = prettyOfParam(
      _.error
    )
  }

  /** Indicates that the transaction could not be converted to a transaction tree.
    * @see TransactionTreeFactory.TransactionTreeConversionError for more information.
    */
  final case class TransactionTreeFactoryError(cause: TransactionTreeConversionError)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[TransactionTreeFactoryError] = prettyOfParam(_.cause)
  }

  final case class RecipientsCreationError(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[RecipientsCreationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class LightTransactionViewTreeCreationError(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[LightTransactionViewTreeCreationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class TransactionSigningError(cause: SyncCryptoError)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[TransactionSigningError] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }
}
