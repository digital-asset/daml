// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.Applicative
import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.ledger.api.v2.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v2.transaction.Transaction as ApiTransaction
import com.daml.ledger.javaapi as javab
import com.daml.ledger.javaapi.data.Transaction
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
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
import com.digitalasset.canton.{LfPackageId, SynchronizerAlias, config}
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
    ): ExternalParty = {

      val onboardingET = for {
        psid <- EitherT
          .fromEither[FutureUnlessShutdown](lookupOrDetectSynchronizerId(synchronizer))
          .leftMap(err => s"Cannot find physical synchronizer id: $err")

        onboardingData <- onboarding_transactions(name, synchronizer)
        (onboardingTxs, externalParty) = onboardingData

        _ = reference.topology.transactions.load(
          onboardingTxs.toSeq,
          psid,
          synchronize = synchronize,
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

        protocolSigningKey <- crypto
          .generateSigningKey(usage = SigningKeyUsage.ProtocolOnly)
          .leftMap(_.toString)
        partyToKeyTx = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          PartyToKeyMapping.tryCreate(
            partyId = partyId,
            threshold = PositiveInt.one,
            signingKeys = NonEmpty.mk(Seq, protocolSigningKey),
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
        protocolSignature <- crypto.privateCrypto
          .sign(
            partyToKeyTx.hash.hash,
            protocolSigningKey.fingerprint,
            NonEmpty.mk(Set, SigningKeyUsage.Protocol),
          )
          .leftMap(_.toString)

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
          .addSingleSignature(protocolSignature)
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
          ExternalParty(partyId, NonEmpty.mk(Seq, protocolSigningKey.fingerprint)),
        )
      }

    /** Sign the given hash on behalf of the external party
      */
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

          val preparedTransaction = prepared.preparedTransaction.getOrElse(
            consoleEnvironment.raiseError("Prepared transaction was empty")
          )

          val signatures = Map(actAs.partyId -> sign(prepared.preparedTransactionHash, actAs))

          val tx = reference.ledger_api.interactive_submission
            .execute_and_wait_for_transaction(
              preparedTransaction = preparedTransaction,
              transactionSignatures = signatures,
              submissionId = submissionId,
              hashingSchemeVersion = prepared.hashingSchemeVersion,
              // TODO(#27482) Check whether this can be removed and unified with local parties case
              transactionFormat = None,
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
