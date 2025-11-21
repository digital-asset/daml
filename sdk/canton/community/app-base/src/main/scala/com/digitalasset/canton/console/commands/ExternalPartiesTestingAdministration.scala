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
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeyUsage}
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
  PartyToKeyMapping,
  PartyToParticipant,
  SignedTopologyTransaction,
  SingleTransactionSignature,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  ExternalParty,
  KnownPhysicalSynchronizerId,
  Namespace,
  ParticipantId,
  Party,
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
  private[canton] def enable(
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

  /** Utility method to create a namespace delegation controlled by an external key. Use to create
    * namespaces prior to allocating an external party controlled by a decentralized namespace for
    * instance.
    * @param synchronizer
    *   Synchronizer
    * @return
    *   Namespace
    */
  @VisibleForTesting
  private[canton] def create_external_namespace(
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
  @VisibleForTesting
  private[canton] def also_enable(
      party: Party,
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
  @VisibleForTesting
  private def onboarding_transactions_for_existing(
      party: Party,
      synchronizer: SynchronizerAlias,
      confirmationThreshold: PositiveInt,
      additionalConfirming: Seq[ParticipantId] = Seq.empty,
      observing: Seq[ParticipantId] = Seq.empty,
  ): EitherT[FutureUnlessShutdown, String, OnboardingTransactions] = {
    val knownPSIds = reference.synchronizers.list_registered().collect {
      case (_, KnownPhysicalSynchronizerId(psid), _) => psid
    }

    def getUniqueMapping[T](
        getter: PhysicalSynchronizerId => Seq[T],
        key: String,
    ): EitherT[FutureUnlessShutdown, String, T] =
      knownPSIds.flatMap(getter).toList.distinct match {
        case Nil => EitherT.leftT[FutureUnlessShutdown, T](s"Unable to $key for $party")
        case head :: Nil => EitherT.rightT[FutureUnlessShutdown, String](head)
        case _several => EitherT.leftT[FutureUnlessShutdown, T](s"Found several $key for $party")
      }

    for {
      protocolVersion <- EitherT
        .fromEither[FutureUnlessShutdown](
          lookupOrDetectSynchronizerId(Some(synchronizer)).map(_.protocolVersion)
        )
        .leftMap(err => s"Cannot find protocol version: $err")

      namespaceDelegation <- getUniqueMapping(
        psid =>
          reference.topology.namespace_delegations
            .list(store = psid, filterNamespace = party.namespace.filterString)
            .map(_.item),
        "namespace delegation",
      )

      namespaceDelegationTx = TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        namespaceDelegation,
        protocolVersion,
      )

      partyToKey <- getUniqueMapping(
        psid =>
          reference.topology.party_to_key_mappings
            .list(store = psid, filterParty = party.filterString)
            .map(_.item),
        "party to key",
      )

      partyToKeyTx = TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        partyToKey,
        protocolVersion,
      )

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
          partyId = party.partyId,
          threshold = confirmationThreshold,
          participants = hostingConfirming ++ hostingObserving,
        ),
        protocolVersion,
      )

      onboardingTransactions <- bundle_onboarding_transactions(
        partyId = party.partyId,
        protocolSigningKeys = partyToKeyTx.mapping.signingKeys.map(_.fingerprint).toSeq,
        protocolVersion = protocolVersion,
        namespaceTx = namespaceDelegationTx,
        partyToKeyTx = partyToKeyTx,
        partyToParticipantTx = partyToParticipantTx,
        decentralizedNamespaceOwners = Set.empty,
      )

    } yield onboardingTransactions
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
            val updatedParty = reference.topology.party_to_key_mappings
              .list(
                singleSynchronizer,
                filterParty = result.party.filterString,
              )
              .headOption
              .map { p2k =>
                ExternalParty(
                  result.party,
                  p2k.item.signingKeys.map(_.fingerprint).toSeq,
                  p2k.item.threshold,
                )
              }
              .getOrElse(result.party)
            ListPartiesResult(updatedParty, result.participants)
          case _ => result
        }
      }

  /** Sign the onboarding transactions and build a [[OnboardingTransactions]]
    */
  @VisibleForTesting
  private[commands] def bundle_onboarding_transactions(
      partyId: PartyId,
      protocolSigningKeys: NonEmpty[Seq[Fingerprint]],
      protocolVersion: ProtocolVersion,
      namespaceTx: TopologyTransaction[TopologyChangeOp.Replace, TopologyMapping],
      partyToKeyTx: TopologyTransaction[TopologyChangeOp.Replace, PartyToKeyMapping],
      partyToParticipantTx: TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      decentralizedNamespaceOwners: Set[Namespace],
  ): EitherT[FutureUnlessShutdown, String, OnboardingTransactions] = {
    val transactionHashes = NonEmpty.mk(
      Set,
      namespaceTx.hash,
      partyToParticipantTx.hash,
      partyToKeyTx.hash,
    )

    val combinedMultiTxHash = MultiTransactionSignature.computeCombinedHash(
      transactionHashes,
      consoleEnvironment.tryGlobalCrypto.pureCrypto,
    )

    val decentralizedOwnersNEO = NonEmpty.from(decentralizedNamespaceOwners)

    val namespaceFingerprints = decentralizedOwnersNEO
      .map(_.map(_.fingerprint))
      .getOrElse(NonEmpty.mk(Set, partyId.fingerprint))

    // Sign the multi hash with the namespace keys, as it is needed to authorize all transactions
    val namespaceSignatures = namespaceFingerprints.toSeq.map(
      consoleEnvironment.global_secret.sign(
        combinedMultiTxHash.getCryptographicEvidence,
        _,
        NonEmpty.mk(Set, SigningKeyUsage.Namespace: SigningKeyUsage),
      )
    )

    for {
      // The protocol key signature is only needed on the party to key mapping, so we can sign only that
      protocolSignatures <- protocolSigningKeys.toNEF
        .parTraverse { protocolSigningKey =>
          consoleEnvironment.tryGlobalCrypto.privateCrypto
            .sign(
              partyToKeyTx.hash.hash,
              protocolSigningKey,
              NonEmpty.mk(Set, SigningKeyUsage.Protocol),
            )
        }
        .leftMap(_.toString)
        .map(_.toSeq)

      multiTxSignatures = namespaceSignatures.map(namespaceSignature =>
        MultiTransactionSignature(transactionHashes, namespaceSignature)
      )

      signedNamespace = SignedTopologyTransaction
        .withTopologySignatures(
          namespaceTx,
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
        "namespace" -> signedNamespace,
        "party-to-participant" -> signedPartyToParticipant,
        "party-to-key" -> signedPartyToKey,
      ).view.mapValues(_.signatures.map(_.authorizingLongTermKey).mkString(", "))

      logger.info(
        s"Generated onboarding transactions for external party ${partyId.identifier} with id $partyId: $keys"
      )

      OnboardingTransactions(
        signedNamespace,
        signedPartyToParticipant,
        signedPartyToKey,
      )
    }
  }

  /** Generate the party id and namespace transaction for a centralized namespace party. Creates the
    * namespace key in the global crypto store.
    */
  private def build_centralized_namespace(
      name: String,
      protocolVersion: ProtocolVersion,
  ): EitherT[
    FutureUnlessShutdown,
    String,
    (PartyId, TopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]),
  ] = for {
    namespaceKey <- consoleEnvironment.tryGlobalCrypto
      .generateSigningKey(usage = SigningKeyUsage.NamespaceOnly)
      .leftMap(_.toString)
    partyId = PartyId.tryCreate(name, namespaceKey.fingerprint)
    mapping <- EitherT.fromEither[FutureUnlessShutdown](
      NamespaceDelegation.create(
        namespace = partyId.namespace,
        target = namespaceKey,
        CanSignAllMappings,
      )
    )
    namespaceTx = TopologyTransaction(
      TopologyChangeOp.Replace,
      serial = PositiveInt.one,
      mapping,
      protocolVersion,
    )
  } yield (partyId, namespaceTx)

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
      TopologyTransaction[TopologyChangeOp.Replace, TopologyMapping],
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
  @VisibleForTesting
  private[canton] def onboarding_transactions(
      name: String,
      synchronizer: Option[SynchronizerAlias] = None,
      additionalConfirming: Seq[ParticipantId] = Seq.empty,
      observing: Seq[ParticipantId] = Seq.empty,
      confirmationThreshold: PositiveInt = PositiveInt.one,
      keysCount: PositiveInt = PositiveInt.one,
      keysThreshold: PositiveInt = PositiveInt.one,
      decentralizedNamespaceOwners: Set[Namespace] = Set.empty,
      namespaceThreshold: PositiveInt = PositiveInt.one,
  ): EitherT[FutureUnlessShutdown, String, (OnboardingTransactions, ExternalParty)] =
    for {
      protocolVersion <- EitherT
        .fromEither[FutureUnlessShutdown](
          lookupOrDetectSynchronizerId(synchronizer).map(_.protocolVersion)
        )
        .leftMap(err => s"Cannot find protocol version: $err")

      decentralizedOwnersNEO = NonEmpty.from(decentralizedNamespaceOwners)

      partyIdAndNamespaceTx <- decentralizedOwnersNEO
        .map(namespaceOwnersNE =>
          EitherT.pure[FutureUnlessShutdown, String](
            build_decentralized_namespace(
              name,
              protocolVersion,
              namespaceOwnersNE,
              namespaceThreshold,
            )
          )
        )
        .getOrElse(build_centralized_namespace(name, protocolVersion))

      (partyId, namespaceTx) = partyIdAndNamespaceTx

      protocolSigningKeys = consoleEnvironment.global_secret.keys.secret
        .generate_keys(keysCount, usage = SigningKeyUsage.ProtocolOnly)

      partyToKeyTx = TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        PartyToKeyMapping.tryCreate(
          partyId = partyId,
          threshold = keysThreshold,
          signingKeys = protocolSigningKeys,
        ),
        protocolVersion,
      )

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
        ),
        protocolVersion,
      )

      onboardingTransactions <- bundle_onboarding_transactions(
        partyId = partyId,
        protocolSigningKeys = protocolSigningKeys.map(_.fingerprint),
        protocolVersion = protocolVersion,
        namespaceTx = namespaceTx,
        partyToKeyTx = partyToKeyTx,
        partyToParticipantTx = partyToParticipantTx,
        decentralizedNamespaceOwners = decentralizedNamespaceOwners,
      )
    } yield (
      onboardingTransactions,
      ExternalParty(partyId, protocolSigningKeys.map(_.fingerprint), keysThreshold),
    )

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
