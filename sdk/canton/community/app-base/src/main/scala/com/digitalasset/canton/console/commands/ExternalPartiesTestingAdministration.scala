// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.Applicative
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.admin.api.client.data.ListPartiesResult
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  ConsoleCommandResult,
  ConsoleEnvironment,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeyUsage, SigningKeysWithThreshold}
import com.digitalasset.canton.data.OnboardingTransactions
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  HostingParticipant,
  MultiTransactionSignature,
  NamespaceDelegation,
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  SingleTransactionSignature,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  ExternalParty,
  KnownPhysicalSynchronizerId,
  Namespace,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SynchronizerAlias, config}
import com.google.common.annotations.VisibleForTesting

import java.time.Instant
import scala.concurrent.ExecutionContext

/** Subset of commands and external party related logic ONLY USED FOR TESTING, but still part of the
  * console to make it seamlessly usable in integration tests.
  */
@VisibleForTesting
private[canton] class ExternalPartiesTestingAdministration(
    reference: ParticipantReference,
    consoleEnvironment: ConsoleEnvironment,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  private implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext
  private implicit val tc: TraceContext = TraceContext.empty
  private implicit val ce: ConsoleEnvironment = consoleEnvironment
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  /** Enable an external party hosted on `reference` with confirmation rights.
    * @param name
    *   Name of the party to be enabled
    * @param synchronizer
    *   Synchronizer
    * @param synchronizeParticipants
    *   Participants that need to see activation of the party
    */
  @VisibleForTesting
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

      _ = reference.ledger_api.parties.allocate_external(
        psid.logical,
        onboardingTxs.transactionsWithSingleSignature,
        onboardingTxs.multiTransactionSignatures,
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

  @VisibleForTesting
  private[canton] def list(
      filterParty: String = "",
      filterParticipant: String = "",
      synchronizerIds: Set[SynchronizerId] = Set.empty,
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListPartiesResult] =
    reference.parties
      .list(
        filterParty = filterParty,
        filterParticipant = filterParticipant,
        synchronizerIds = synchronizerIds,
        asOf = asOf,
        limit = limit,
      )
      .map { result =>
        // Assume that the external party has the same signing keys on all synchronizers it exists on
        // That's consistent with how [[also_enable]] works
        result.participants
          .flatMap(_.synchronizers.map(_.synchronizerId))
          .distinct
          .headOption match {
          case Some(singleSynchronizer) =>
            val updatedParty = reference.topology.party_to_participant_mappings
              .list(
                singleSynchronizer,
                filterParty = result.party.filterString,
              )
              .headOption
              .flatMap(_.item.partySigningKeysWithThreshold)
              .map { case SigningKeysWithThreshold(keys, threshold) =>
                ExternalParty(
                  result.party,
                  keys.map(_.fingerprint).toSeq,
                  threshold,
                )
              }
              .getOrElse(result.party)
            ListPartiesResult(updatedParty, result.participants)
          case _ => result
        }
      }

  /** Generate the party id and namespace transaction for a decentralized namespace party from a set
    * of existing namespaces. The namespaces must already exist and be authorized in the topology of
    * the target synchronizer.
    */
  private def build_decentralized_namespace(
      name: String,
      protocolVersion: ProtocolVersion,
      namespaceOwners: NonEmpty[Set[Namespace]],
      namespaceThreshold: PositiveInt,
  ): (
      PartyId,
      TopologyTransaction[TopologyChangeOp.Replace, DecentralizedNamespaceDefinition],
  ) = {
    val decentralizedNamespace =
      DecentralizedNamespaceDefinition.computeNamespace(namespaceOwners.forgetNE)
    val partyId = PartyId.tryCreate(name, decentralizedNamespace.fingerprint)
    val namespaceTx = TopologyTransaction(
      TopologyChangeOp.Replace,
      serial = PositiveInt.one,
      DecentralizedNamespaceDefinition.tryCreate(
        decentralizedNamespace,
        namespaceThreshold,
        namespaceOwners,
      ),
      protocolVersion,
    )
    (partyId, namespaceTx)
  }

  /** Utility method to create a namespace delegation controlled by an external key. Use to create
    * namespaces prior to allocating an external party controlled by a decentralized namespace for
    * instance.
    * @param synchronizer
    *   Synchronizer
    * @return
    *   Namespace
    */
  @VisibleForTesting // Ensures external this is only used in testing
  def create_external_namespace(
      synchronizer: Option[SynchronizerAlias] = None,
      synchronize: Option[config.NonNegativeDuration] = Some(timeouts.unbounded),
  ): Namespace = {
    val res = for {
      psid <- EitherT
        .fromEither[FutureUnlessShutdown](
          lookupOrDetectSynchronizerId(synchronizer)
        )
        .leftMap(err => s"Cannot find protocol version: $err")

      namespaceKey <- consoleEnvironment.tryGlobalCrypto
        .generateSigningKey(usage = SigningKeyUsage.NamespaceOnly)
        .leftMap(_.toString)

      namespace = Namespace(namespaceKey.fingerprint)

      namespaceTx = TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        NamespaceDelegation.tryCreate(
          namespace = namespace,
          target = namespaceKey,
          CanSignAllMappings,
        ),
        psid.protocolVersion,
      )

      signature <- consoleEnvironment.tryGlobalCrypto.privateCrypto
        .sign(
          namespaceTx.hash.hash,
          namespaceKey.fingerprint,
          NonEmpty.mk(Set, SigningKeyUsage.Namespace),
        )
        .leftMap(_.toString)

      signedNamespace = SignedTopologyTransaction
        .withTopologySignatures(
          namespaceTx,
          NonEmpty.mk(Seq, SingleTransactionSignature(namespaceTx.hash, signature)),
          isProposal = false,
          psid.protocolVersion,
        )

      _ = reference.topology.transactions.load(
        Seq(signedNamespace),
        psid,
        synchronize = synchronize,
      )
    } yield Namespace(namespaceKey.fingerprint)

    consoleEnvironment.run(ConsoleCommandResult.fromEitherTUS(res))
  }

  /** Compute the onboarding transaction to enable party `name`
    * @param name
    *   Name of the party to be enabled
    * @param synchronizer
    *   Synchronizer
    * @param additionalConfirming
    *   Other confirming participants
    * @param observing
    *   Observing participants
    * @param decentralizedNamespaceOwners
    *   Set when creating a party controlle by a decentralized namespace. The namespaces must
    *   already exist and be authorized in the topology of the target synchronizer.
    * @param namespaceThreshold
    *   Threshold of the decentralized namespace. Only used when decentralizedNamespaceOwners is non
    *   empty.
    */
  @VisibleForTesting // Ensures external parties are created only in tests
  def onboarding_transactions(
      name: String,
      synchronizer: Option[SynchronizerAlias] = None,
      additionalConfirming: Seq[ParticipantId] = Seq.empty,
      observing: Seq[ParticipantId] = Seq.empty,
      confirmationThreshold: PositiveInt = PositiveInt.one,
      keysCount: PositiveInt = PositiveInt.one,
      keysThreshold: PositiveInt = PositiveInt.one,
      decentralizedNamespaceOwners: Set[Namespace] = Set.empty,
      namespaceThreshold: PositiveInt = PositiveInt.one,
  ): EitherT[FutureUnlessShutdown, String, (OnboardingTransactions, ExternalParty)] = {
    // In the simple case of a non-decentralized party with a single key we can use the generate endpoint
    if (decentralizedNamespaceOwners.isEmpty && keysCount == PositiveInt.one) {
      for {
        synchronizerId <- EitherT
          .fromEither[FutureUnlessShutdown](
            lookupOrDetectSynchronizerId(synchronizer)
          )
          .leftMap(err => s"Cannot find protocol version: $err")
        partyKey <- consoleEnvironment.tryGlobalCrypto
          .generateSigningKey(usage =
            NonEmpty.mk(Set, SigningKeyUsage.Namespace, SigningKeyUsage.Protocol)
          )
          .leftMap(_.toString)
        partyId = PartyId.tryCreate(name, partyKey.fingerprint)
        onboardingTransactions = reference.ledger_api.parties.generate_topology(
          synchronizerId = synchronizerId.logical,
          partyHint = name,
          publicKey = partyKey,
          otherConfirmingParticipantIds = additionalConfirming,
          confirmationThreshold = confirmationThreshold.toNonNegative,
          observingParticipantIds = observing,
        )
        centralizedPTP <- EitherT.fromOption[FutureUnlessShutdown](
          onboardingTransactions.topologyTransactions.flatMap {
            _.selectMapping[PartyToParticipant]
          }.headOption,
          "Expected a PartyToParticipant onboarding transaction from the generate_topology command",
        )
        bundledTransaction <- bundle_onboarding_transactions(
          partyId,
          NonEmpty.mk(Seq, partyKey.fingerprint),
          synchronizerId.protocolVersion,
          optionalDecentralizedNamespaceTx = None,
          centralizedPTP,
          decentralizedNamespaceOwners = Set.empty,
        )
      } yield (
        bundledTransaction,
        ExternalParty(partyId, NonEmpty.mk(Seq, partyKey.fingerprint), keysThreshold),
      )
    } else {
      for {
        synchronizerId <- EitherT
          .fromEither[FutureUnlessShutdown](
            lookupOrDetectSynchronizerId(synchronizer)
          )
          .leftMap(err => s"Cannot find synchronizer id: $err")
        protocolVersion = synchronizerId.protocolVersion
        decentralizedOwnersNEO = NonEmpty.from(decentralizedNamespaceOwners)

        // The party's namespace key
        // Needs both Namespace and Protocol usage as it is both the key controlling the party's namespace
        // and a protocol signing key
        namespaceSigningKey = consoleEnvironment.global_secret.keys.secret
          .generate_keys(
            PositiveInt.one,
            usage = NonEmpty.mk(Set, SigningKeyUsage.Namespace, SigningKeyUsage.Protocol),
          )
          .head1

        protocolSigningKeys = keysCount.decrement.toPositiveNumeric match {
          // If we want only one key, then it's also the only protocol signing key
          case None => NonEmpty.mk(Seq, namespaceSigningKey)
          // Otherwise create the rest of the protocol keys with protocol usage only
          case Some(additionalProtocolKeysCount) =>
            val additionalKeys = consoleEnvironment.global_secret.keys.secret
              .generate_keys(
                additionalProtocolKeysCount,
                usage = NonEmpty.mk(Set, SigningKeyUsage.Protocol),
              )
            NonEmpty.mk(Seq, namespaceSigningKey, additionalKeys*)
        }

        (partyId, maybeDecentralizedNamespaceTx) = decentralizedOwnersNEO
          .map(namespaceOwnersNE =>
            build_decentralized_namespace(
              name,
              protocolVersion,
              namespaceOwnersNE,
              namespaceThreshold,
            )
          )
          .map { case (partyId, decentralizedNamespaceTx) =>
            (partyId, Some(decentralizedNamespaceTx))
          }
          .getOrElse {
            val partyId = PartyId.tryCreate(name, namespaceSigningKey.fingerprint)
            (partyId, None)
          }

        hybridParticipants = additionalConfirming.intersect(observing)
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          NonEmpty
            .from(hybridParticipants)
            .toLeft(())
            .leftMap(hybridParticipants =>
              s"The following participants are indicated as observing and confirming: $hybridParticipants"
            )
        )

        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          !observing.contains(reference.id),
          "reference participant should not be observing",
        )

        hostingConfirming = (reference.id +: additionalConfirming).map(
          HostingParticipant(_, ParticipantPermission.Confirmation)
        )

        hostingObserving = observing.map(
          HostingParticipant(_, ParticipantPermission.Observation)
        )

        partyToParticipantTx = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          PartyToParticipant.tryCreate(
            partyId = partyId,
            threshold = confirmationThreshold,
            participants = hostingConfirming ++ hostingObserving,
            partySigningKeysWithThreshold = Some(
              SigningKeysWithThreshold.tryCreate(
                threshold = keysThreshold,
                keys = protocolSigningKeys,
              )
            ),
          ),
          protocolVersion,
        )

        onboardingTransactions <- bundle_onboarding_transactions(
          partyId = partyId,
          protocolSigningKeys = protocolSigningKeys.map(_.fingerprint),
          protocolVersion = protocolVersion,
          optionalDecentralizedNamespaceTx = maybeDecentralizedNamespaceTx,
          partyToParticipantTx = partyToParticipantTx,
          decentralizedNamespaceOwners = decentralizedNamespaceOwners,
        )
      } yield (
        onboardingTransactions,
        ExternalParty(partyId, protocolSigningKeys.map(_.fingerprint), keysThreshold),
      )
    }
  }

  /** Enable an existing external party hosted on `reference` with confirmation rights. Unlike
    * `enable`, this command assumes the external party already exists on a different synchronizer.
    *
    * Note: the keys (and threshold) will be the same as on the synchronizer on which the party is
    * already hosted.
    *
    * @param party
    *   Name of the party to be enabled
    * @param synchronizer
    *   Synchronizer
    * @param synchronizeParticipants
    *   Participants that need to see activation of the party
    */
  @VisibleForTesting // Ensures external parties are created only in tests
  def also_enable(
      party: ExternalParty,
      synchronizer: SynchronizerAlias,
      synchronizeParticipants: Seq[ParticipantReference] = consoleEnvironment.participants.all,
      synchronize: Option[config.NonNegativeDuration] = Some(timeouts.unbounded),
  ): Unit = {

    val onboardingET = for {
      psid <- EitherT
        .fromEither[FutureUnlessShutdown](lookupOrDetectSynchronizerId(Some(synchronizer)))
        .leftMap(err => s"Cannot find physical synchronizer id: $err")

      onboardingTxs <- onboarding_transactions_for_existing(
        party,
        synchronizer,
        confirmationThreshold = PositiveInt.one,
      )

      _ = reference.ledger_api.parties.allocate_external(
        psid.logical,
        onboardingTxs.transactionsWithSingleSignature,
        onboardingTxs.multiTransactionSignatures,
      )

      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Applicative[Either[String, *]].whenA(synchronize.nonEmpty)(
          PartiesAdministration.Allocation.waitForPartyKnown(
            partyId = party.partyId,
            hostingParticipant = reference,
            synchronizeParticipants = synchronizeParticipants,
            synchronizerId = psid.logical,
          )(consoleEnvironment)
        )
      )
    } yield ()

    consoleEnvironment.run(ConsoleCommandResult.fromEitherTUS(onboardingET))
  }

  /** Compute onboarding transactions for an existing external party.
    *
    * Note: the keys (and threshold) will be the same as on the synchronizer on which the party is
    * already hosted.
    * @param name
    *   Name of the party to be enabled
    * @param synchronizer
    *   Synchronizer
    * @param additionalConfirming
    *   Other confirming participants
    * @param observing
    *   Observing participants
    */
  @VisibleForTesting // Ensures external parties are used only in tests
  private def onboarding_transactions_for_existing(
      party: ExternalParty,
      synchronizer: SynchronizerAlias,
      confirmationThreshold: PositiveInt,
      additionalConfirming: Seq[ParticipantId] = Seq.empty,
      observing: Seq[ParticipantId] = Seq.empty,
  ): EitherT[FutureUnlessShutdown, String, OnboardingTransactions] = {
    val knownPSIds = reference.synchronizers.list_registered().collect {
      case (_, KnownPhysicalSynchronizerId(psid), _) => psid
    }

    for {
      protocolVersion <- EitherT
        .fromEither[FutureUnlessShutdown](
          lookupOrDetectSynchronizerId(Some(synchronizer)).map(_.protocolVersion)
        )
        .leftMap(err => s"Cannot find protocol version: $err")

      hybridParticipants = additionalConfirming.intersect(observing)
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        NonEmpty
          .from(hybridParticipants)
          .toLeft(())
          .leftMap(hybridParticipants =>
            s"The following participants are indicated as observing and confirming: $hybridParticipants"
          )
      )

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        !observing.contains(reference.id),
        "reference participant should not be observing",
      )

      // Find the first PTP with keys for this party on any known synchronizer
      signingKeys <- EitherT.fromEither[FutureUnlessShutdown](
        knownPSIds
          .flatMap(psid =>
            reference.topology.party_to_participant_mappings
              .list(psid.logical, filterParty = party.partyId.filterString)
          )
          .headOption
          .flatMap(_.item.partySigningKeysWithThreshold)
          .toRight(s"Unable to find a synchronizer with a PTP with signing keys for $party")
      )

      hostingConfirming = (reference.id +: additionalConfirming).map(
        HostingParticipant(_, ParticipantPermission.Confirmation)
      )

      hostingObserving = observing.map(
        HostingParticipant(_, ParticipantPermission.Observation)
      )

      partyToParticipantTx = TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        PartyToParticipant.tryCreate(
          partyId = party.partyId,
          threshold = confirmationThreshold,
          participants = hostingConfirming ++ hostingObserving,
          partySigningKeysWithThreshold = Some(signingKeys),
        ),
        protocolVersion,
      )

      onboardingTransactions <- bundle_onboarding_transactions(
        partyId = party.partyId,
        protocolSigningKeys = party.signingFingerprints,
        protocolVersion = protocolVersion,
        optionalDecentralizedNamespaceTx = None,
        partyToParticipantTx = partyToParticipantTx,
        decentralizedNamespaceOwners = Set.empty,
      )

    } yield onboardingTransactions
  }

  /** Sign the onboarding transactions and build a [[OnboardingTransactions]]
    */
  private def bundle_onboarding_transactions(
      partyId: PartyId,
      protocolSigningKeys: NonEmpty[Seq[Fingerprint]],
      protocolVersion: ProtocolVersion,
      optionalDecentralizedNamespaceTx: Option[
        TopologyTransaction[TopologyChangeOp.Replace, DecentralizedNamespaceDefinition]
      ],
      partyToParticipantTx: TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      decentralizedNamespaceOwners: Set[Namespace],
  ): EitherT[FutureUnlessShutdown, String, OnboardingTransactions] = {
    val transactionHashes = NonEmpty.mk(
      Set,
      partyToParticipantTx.hash,
      optionalDecentralizedNamespaceTx.toList.map(_.hash)*
    )

    val authorizingHash = MultiTransactionSignature.computeCombinedHash(
      transactionHashes,
      consoleEnvironment.tryGlobalCrypto.pureCrypto,
    )

    val decentralizedOwnersNEO = NonEmpty.from(decentralizedNamespaceOwners)

    val namespaceFingerprints = decentralizedOwnersNEO
      .map(_.map(_.fingerprint))
      .getOrElse(NonEmpty.mk(Set, partyId.fingerprint))

    // Sign the multi hash with the namespace keys, as it is needed to authorize all transactions
    val namespaceSignatures = namespaceFingerprints.toSeq.map { key =>
      consoleEnvironment.global_secret.sign(
        authorizingHash.getCryptographicEvidence,
        key,
        NonEmpty.mk(Set, SigningKeyUsage.Namespace: SigningKeyUsage),
      )
    }

    for {
      // The protocol key signature is only needed on the party to participant mapping, so we can sign only that
      protocolSignatures <- protocolSigningKeys.toNEF
        .parTraverse { protocolSigningKey =>
          consoleEnvironment.tryGlobalCrypto.privateCrypto
            .sign(
              partyToParticipantTx.hash.hash,
              protocolSigningKey,
              NonEmpty.mk(Set, SigningKeyUsage.Protocol),
            )
        }
        .leftMap(_.toString)
        .map(_.toSeq)

      multiTxSignatures = namespaceSignatures.map(namespaceSignature =>
        MultiTransactionSignature(transactionHashes, namespaceSignature)
      )

      signedDecentralizedNamespaceO = optionalDecentralizedNamespaceTx.map {
        SignedTopologyTransaction
          .withTopologySignatures(
            _,
            multiTxSignatures,
            isProposal = false,
            protocolVersion,
          )
      }

      signedPartyToParticipant = SignedTopologyTransaction
        .withTopologySignatures(
          partyToParticipantTx,
          multiTxSignatures,
          isProposal = true,
          protocolVersion,
        ) // Merge the signature from the protocol key
        .addSingleSignatures(protocolSignatures.toSet)
    } yield {
      val keys = (
        signedDecentralizedNamespaceO.map("decentralized-namespace" -> _).toList.toMap ++
          Map("party-to-participant" -> signedPartyToParticipant)
      ).view.mapValues(_.signatures.map(_.authorizingLongTermKey).mkString(", "))

      logger.info(
        s"Generated onboarding transactions for external party ${partyId.identifier} with id $partyId: $keys"
      )

      OnboardingTransactions(
        signedPartyToParticipant,
        signedDecentralizedNamespaceO,
      )
    }
  }

  /** @return
    *   if SynchronizerAlias is set, the SynchronizerId that corresponds to the alias. if
    *   SynchronizerAlias is not set, the synchronizer id of the only connected synchronizer. if the
    *   participant is connected to multiple synchronizers, it returns an error.
    */
  private def lookupOrDetectSynchronizerId(
      alias: Option[SynchronizerAlias]
  ): Either[String, PhysicalSynchronizerId] = {
    lazy val singleConnectedSynchronizer = reference.synchronizers.list_connected() match {
      case Seq() =>
        Left("not connected to any synchronizer")
      case Seq(onlyOneSynchronizer) => Right(onlyOneSynchronizer.physicalSynchronizerId)
      case multiple =>
        val psids = multiple.map(_.physicalSynchronizerId)

        Left(
          s"cannot automatically determine synchronizer, because participant is connected to more than 1 synchronizer: $psids"
        )
    }
    alias
      .map(a => Right(reference.synchronizers.physical_id_of(a)))
      .getOrElse(singleConnectedSynchronizer)
  }
}
