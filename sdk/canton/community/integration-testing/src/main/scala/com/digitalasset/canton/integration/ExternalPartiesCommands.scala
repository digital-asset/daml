// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.Applicative
import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.ledger.api.v2.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.PrepareSubmissionResponse as PrepareResponseProto
import com.daml.ledger.api.v2.transaction.Transaction as ApiTransaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi as javab
import com.daml.ledger.javaapi.data.Transaction
import com.daml.nonempty.catsinstances.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ConsoleMacros.utils
import com.digitalasset.canton.console.commands.{
  LocalSecretKeyAdministration,
  PartiesAdministration,
}
import com.digitalasset.canton.console.{
  ConsoleCommandResult,
  ConsoleEnvironment,
  Help,
  Helpful,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, Signature, SigningKeyUsage}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.interactive.ExternalPartyUtils.OnboardingTransactions
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  MultiTransactionSignature,
  NamespaceDelegation,
  ParticipantPermission,
  PartyToKeyMapping,
  PartyToParticipant,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  ExternalParty,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPackageId, SynchronizerAlias, checked, config}
import com.google.protobuf.ByteString

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

/** This class contains a copy of some of the standard console commands for local parties. Goals
  * are:
  *   - Make it easy to write tests for external parties.
  *   - Have commands that are as similar as possible to the ones for local parties.
  *
  * Assumptions:
  *   - External parties are allocated using `external_parties.enable` below (so that the key is in
  *     `crypto`.
  */
class ExternalPartiesCommands(
    reference: ParticipantReference,
    crypto: Crypto,
    protected val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with Helpful {

  private implicit val ce: ConsoleEnvironment = consoleEnvironment
  private implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext
  private implicit val traceContext: TraceContext = TraceContext.empty
  private val timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  // TODO(#27482) Maybe it should not be external parties: what matters is that the party lives in its namespace
  object external_parties {

    /** Enable an external party hosted on `reference` with confirmation rights.
      * @param name
      *   Name of the party to be enabled
      * @param synchronizer
      *   Synchronizer
      * @param synchronizeParticipants
      *   Participants that need to see activation of the party
      */
    def enable(
        name: String,
        synchronizer: Option[SynchronizerAlias] = None,
        synchronizeParticipants: Seq[ParticipantReference] = consoleEnvironment.participants.all,
        synchronize: Option[config.NonNegativeDuration] = Some(timeouts.unbounded),
        // External party specifics
        keysCount: PositiveInt = PositiveInt.one,
        keysThreshold: PositiveInt = PositiveInt.one,
    ): ExternalParty = {

      val onboardingET = for {
        psid <- EitherT
          .fromEither[FutureUnlessShutdown](lookupOrDetectSynchronizerId(synchronizer))
          .leftMap(err => s"Cannot find physical synchronizer id: $err")

        onboardingData <- onboarding_transactions(
          name,
          synchronizer,
          keysCount = keysCount,
          keysThreshold = keysThreshold,
        )
        (onboardingTxs, externalParty) = onboardingData

        _ = reference.topology.transactions.load(
          onboardingTxs.toSeq,
          psid,
          synchronize = synchronize,
        )

        // Wait until the proposal is known
        _ = utils.retry_until_true(
          reference.topology.party_to_participant_mappings
            .list(
              psid,
              proposals = true,
              filterParticipant = reference.id.filterString,
              filterParty = externalParty.filterString,
            )
            .nonEmpty
        )

        _ = reference.topology.transactions.authorize[PartyToParticipant](
          txHash = onboardingTxs.partyToParticipant.hash,
          mustBeFullyAuthorized = true,
          store = psid,
        )

        _ <- EitherT.fromEither[FutureUnlessShutdown](
          Applicative[Either[String, *]].whenA(synchronize.nonEmpty)(
            PartiesAdministration.Allocation.waitForPartyKnown(
              partyId = externalParty.partyId,
              hostingParticipant = reference,
              synchronizeParticipants = synchronizeParticipants,
              synchronizerId = psid.logical,
            )(consoleEnvironment)
          )
        )
      } yield externalParty

      consoleEnvironment.run(ConsoleCommandResult.fromEitherTUS(onboardingET))
    }

    /** Compute the onboarding transaction to enable party `name`
      * @param name
      *   Name of the party to be enabled
      * @param synchronizer
      *   Synchronizer
      */
    def onboarding_transactions(
        name: String,
        synchronizer: Option[SynchronizerAlias] = None,
        keysCount: PositiveInt = PositiveInt.one,
        keysThreshold: PositiveInt = PositiveInt.one,
    ): EitherT[FutureUnlessShutdown, String, (OnboardingTransactions, ExternalParty)] =
      for {
        protocolVersion <- EitherT
          .fromEither[FutureUnlessShutdown](
            lookupOrDetectSynchronizerId(synchronizer).map(_.protocolVersion)
          )
          .leftMap(err => s"Cannot find protocol version: $err")

        namespaceKey <- crypto
          .generateSigningKey(usage = SigningKeyUsage.NamespaceOnly)
          .leftMap(_.toString)
        partyId = PartyId.tryCreate(name, namespaceKey.fingerprint)

        namespaceDelegationTx = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          NamespaceDelegation.tryCreate(
            namespace = partyId.namespace,
            target = namespaceKey,
            CanSignAllMappings,
          ),
          protocolVersion,
        )

        protocolSigningKeys <- Seq
          .fill(keysCount.unwrap)(
            crypto
              .generateSigningKey(usage = SigningKeyUsage.ProtocolOnly)
              .leftMap(_.toString)
          )
          .parSequence

        protocolSigningKeysNE = NonEmptyUtil.fromUnsafe(
          // keysCount is positive
          checked(protocolSigningKeys)
        )

        partyToKeyTx = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          PartyToKeyMapping.tryCreate(
            partyId = partyId,
            threshold = keysThreshold,
            signingKeys = protocolSigningKeysNE,
          ),
          protocolVersion,
        )

        partyToParticipantTx = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          PartyToParticipant.tryCreate(
            partyId = partyId,
            threshold = PositiveInt.one,
            participants = Seq(HostingParticipant(reference.id, ParticipantPermission.Confirmation)),
          ),
          protocolVersion,
        )

        transactionHashes = NonEmpty.mk(
          Set,
          namespaceDelegationTx.hash,
          partyToParticipantTx.hash,
          partyToKeyTx.hash,
        )
        combinedMultiTxHash = MultiTransactionSignature.computeCombinedHash(
          transactionHashes,
          crypto.pureCrypto,
        )

        // Sign the multi hash with the namespace key, as it is needed to authorize all transactions
        namespaceSignature <- crypto.privateCrypto
          .sign(
            combinedMultiTxHash,
            namespaceKey.fingerprint,
            NonEmpty.mk(Set, SigningKeyUsage.Namespace),
          )
          .leftMap(_.toString)

        // The protocol key signature is only needed on the party to key mapping, so we can sign only that
        protocolSignatures <- protocolSigningKeysNE.toNEF
          .parTraverse { protocolSigningKey =>
            crypto.privateCrypto
              .sign(
                partyToKeyTx.hash.hash,
                protocolSigningKey.fingerprint,
                NonEmpty.mk(Set, SigningKeyUsage.Protocol),
              )
          }
          .leftMap(_.toString)
          .map(_.toSeq)

        multiTxSignatures = NonEmpty.mk(
          Seq,
          MultiTransactionSignature(transactionHashes, namespaceSignature),
        )

        signedNamespaceDelegation = SignedTopologyTransaction
          .withTopologySignatures(
            namespaceDelegationTx,
            multiTxSignatures,
            isProposal = false,
            protocolVersion,
          )

        signedPartyToParticipant = SignedTopologyTransaction
          .withTopologySignatures(
            partyToParticipantTx,
            multiTxSignatures,
            isProposal = true,
            protocolVersion,
          )

        signedPartyToKey = SignedTopologyTransaction
          .withTopologySignatures(
            partyToKeyTx,
            multiTxSignatures,
            isProposal = false,
            protocolVersion,
          )
          // Merge the signature from the protocol key
          .addSingleSignatures(protocolSignatures.toSet)
      } yield {
        val keys = Map(
          "namespace-delegation" -> signedNamespaceDelegation,
          "party-to-participant" -> signedPartyToParticipant,
          "party-to-key" -> signedPartyToKey,
        ).view.mapValues(_.signatures.map(_.authorizingLongTermKey).mkString(", "))

        logger.info(
          s"Generated onboarding transactions for external party $name with id $partyId: $keys"
        )

        (
          OnboardingTransactions(
            signedNamespaceDelegation,
            signedPartyToParticipant,
            signedPartyToKey,
          ),
          ExternalParty(partyId, protocolSigningKeysNE.map(_.fingerprint)),
        )
      }

    /** Sign the given hash on behalf of the external party
      */
    // TODO(#27482) This should be available globally
    def sign(hash: ByteString, party: ExternalParty): Seq[Signature] =
      consoleEnvironment.run(
        ConsoleCommandResult.fromEitherTUS(
          party.signingFingerprints.forgetNE
            .parTraverse(
              crypto.privateCrypto.signBytes(hash, _, SigningKeyUsage.ProtocolOnly)
            )
            .leftMap(_.toString)
        )
      )

    /** Sign the given topology transaction on behalf of the external party
      */
    // TODO(#27482) This should be available globally
    def sign[Op <: TopologyChangeOp, M <: TopologyMapping](
        tx: TopologyTransaction[Op, M],
        party: PartyId,
        protocolVersion: ProtocolVersion,
    ): SignedTopologyTransaction[Op, M] = {
      val signature: Signature = consoleEnvironment.run(
        ConsoleCommandResult.fromEitherTUS(
          crypto.privateCrypto
            .sign(tx.hash.hash, party.fingerprint, SigningKeyUsage.NamespaceOnly)
            .leftMap(_.toString)
        )
      )

      SignedTopologyTransaction
        .withSignature(
          tx,
          signature,
          isProposal = true,
          protocolVersion,
        )
    }

    object keys {
      object secret {
        @Help.Summary("Download key pair")
        def download(
            fingerprint: Fingerprint,
            protocolVersion: ProtocolVersion = ProtocolVersion.latest,
            password: Option[String] = None,
        ): ByteString = {
          val cmd =
            LocalSecretKeyAdministration.download(crypto, fingerprint, protocolVersion, password)

          consoleEnvironment.run(ConsoleCommandResult.fromEitherTUS(cmd)(consoleEnvironment))
        }
      }
    }

    object ledger_api {
      object commands {
        def submit(
            actAs: ExternalParty, // TODO(#27461) Support multiple submitting parties
            commands: Seq[Command],
            synchronizerId: Option[SynchronizerId] = None,
            commandId: String = "",
            optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
            deduplicationPeriod: Option[DeduplicationPeriod] = None,
            submissionId: String = "",
            minLedgerTimeAbs: Option[Instant] = None,
            readAs: Seq[PartyId] = Seq.empty,
            disclosedContracts: Seq[DisclosedContract] = Seq.empty,
            userId: String = reference.userId,
            userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
            transactionShape: TransactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            // External party specifics
            verboseHashing: Boolean = false,
        ): ApiTransaction = {

          val prepared = reference.ledger_api.interactive_submission.prepare(
            actAs = Seq(actAs.partyId),
            commands = commands,
            synchronizerId = synchronizerId,
            commandId = if (commandId.isEmpty) UUID.randomUUID().toString else commandId,
            minLedgerTimeAbs = minLedgerTimeAbs,
            readAs = readAs,
            disclosedContracts = disclosedContracts,
            userId = userId,
            userPackageSelectionPreference = userPackageSelectionPreference,
            verboseHashing = verboseHashing,
            prefetchContractKeys = Seq(),
          )

          submit_prepared(
            preparedTransaction = prepared,
            actAs = actAs,
            optTimeout = optTimeout,
            deduplicationPeriod = deduplicationPeriod,
            submissionId = submissionId,
            minLedgerTimeAbs = minLedgerTimeAbs,
            userId = userId,
            transactionShape = transactionShape,
          )
        }

        def submit_prepared(
            actAs: ExternalParty, // TODO(#27461) Support multiple submitting parties
            preparedTransaction: PrepareResponseProto,
            optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
            deduplicationPeriod: Option[DeduplicationPeriod] = None,
            submissionId: String = "",
            minLedgerTimeAbs: Option[Instant] = None,
            userId: String = reference.userId,
            transactionShape: TransactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        ): ApiTransaction = {

          val prepared = preparedTransaction.preparedTransaction.getOrElse(
            consoleEnvironment.raiseError("Prepared transaction was empty")
          )

          val signatures = Map(
            actAs.partyId -> sign(preparedTransaction.preparedTransactionHash, actAs)
          )

          val tx = reference.ledger_api.interactive_submission
            .execute_and_wait_for_transaction(
              preparedTransaction = prepared,
              transactionSignatures = signatures,
              submissionId = submissionId,
              hashingSchemeVersion = preparedTransaction.hashingSchemeVersion,
              transactionShape = Some(transactionShape),
              userId = userId,
              deduplicationPeriod = deduplicationPeriod,
              minLedgerTimeAbs = minLedgerTimeAbs,
            )

          reference.optionallyAwait(tx, tx.updateId, tx.synchronizerId, optTimeout)
        }
      }

      object javaapi {
        object commands {

          def submit(
              actAs: ExternalParty, // TODO(#27461) Support multiple submitting parties
              commands: Seq[javab.data.Command],
              synchronizerId: Option[SynchronizerId] = None,
              commandId: String = "",
              optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
              deduplicationPeriod: Option[DeduplicationPeriod] = None,
              submissionId: String = "",
              minLedgerTimeAbs: Option[Instant] = None,
              readAs: Seq[PartyId] = Seq.empty,
              disclosedContracts: Seq[javab.data.DisclosedContract] = Seq.empty,
              userId: String = reference.userId,
              userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
              // External party specifics
              verboseHashing: Boolean = false,
          ): Transaction = {
            val protoCommands = commands.map(_.toProtoCommand).map(Command.fromJavaProto)
            val protoDisclosedContracts =
              disclosedContracts.map(c => DisclosedContract.fromJavaProto(c.toProto))

            val tx = ledger_api.commands.submit(
              actAs = actAs,
              commands = protoCommands,
              synchronizerId = synchronizerId,
              commandId = commandId,
              optTimeout = optTimeout,
              deduplicationPeriod = deduplicationPeriod,
              submissionId = submissionId,
              minLedgerTimeAbs = minLedgerTimeAbs,
              readAs = readAs,
              disclosedContracts = protoDisclosedContracts,
              userId = userId,
              userPackageSelectionPreference = userPackageSelectionPreference,
              verboseHashing = verboseHashing,
            )

            javab.data.Transaction.fromProto(ApiTransaction.toJavaProto(tx))
          }

        }
      }
    }

    /** @return
      *   if SynchronizerAlias is set, the SynchronizerId that corresponds to the alias. if
      *   SynchronizerAlias is not set, the synchronizer id of the only connected synchronizer. if
      *   the participant is connected to multiple synchronizers, it returns an error.
      */
    private def lookupOrDetectSynchronizerId(
        alias: Option[SynchronizerAlias]
    ): Either[String, PhysicalSynchronizerId] = {
      lazy val singleConnectedSynchronizer = reference.synchronizers.list_connected() match {
        case Seq() =>
          Left("not connected to any synchronizer")
        case Seq(onlyOneSynchronizer) => Right(onlyOneSynchronizer.physicalSynchronizerId)
        case _multiple =>
          Left(
            "cannot automatically determine synchronizer, because participant is connected to more than 1 synchronizer"
          )
      }

      alias
        .map(a => Right(reference.synchronizers.physical_id_of(a)))
        .getOrElse(singleConnectedSynchronizer)
    }
  }

}
