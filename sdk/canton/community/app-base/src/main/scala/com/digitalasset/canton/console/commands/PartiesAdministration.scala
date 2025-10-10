// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import cats.Applicative
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  TopologyTransactionWrapper,
  UpdateWrapper,
}
import com.digitalasset.canton.admin.api.client.commands.{
  ParticipantAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{AddPartyStatus, ListPartiesResult}
import com.digitalasset.canton.admin.participant.v30.ExportPartyAcsResponse
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.commands.TopologyTxFiltering.{AddedFilter, RevokedFilter}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeyUsage}
import com.digitalasset.canton.data.{CantonTimestamp, OnboardingTransactions}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.grpc.FileStreamObserver
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.{TopologyTransaction, *}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerParticipantId, SynchronizerAlias, config}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.grpc.Context

import java.time.Instant
import scala.concurrent.ExecutionContext

class PartiesAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  protected def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  import runner.*

  @Help.Summary(
    "List active parties, their active participants, and the participants' permissions on synchronizers."
  )
  @Help.Description(
    """Inspect the parties known by this participant as used for synchronisation.
      |The response is built from the timestamped topology transactions of each synchronizer, excluding the
      |authorized store of the given node. For each known party, the list of active
      |participants and their permission on the synchronizer for that party is given.
      |
      filterParty: Filter by parties starting with the given string.
      filterParticipant: Filter for parties that are hosted by a participant with an id starting with the given string
      filterSynchronizerId: Filter by synchronizers whose id starts with the given string.
      asOf: Optional timestamp to inspect the topology state at a given point in time.
      limit: Limit on the number of parties fetched (defaults to canton.parameters.console.default-limit).

      Example: participant1.parties.list(filterParty="alice")
      """
  )
  def list(
      filterParty: String = "",
      filterParticipant: String = "",
      synchronizerIds: Set[SynchronizerId] = Set.empty,
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListPartiesResult] =
    consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Aggregation.ListParties(
          synchronizerIds = synchronizerIds,
          filterParty = filterParty,
          filterParticipant = filterParticipant,
          asOf = asOf,
          limit = limit,
        )
      )
    }
}

class ParticipantPartiesAdministrationGroup(
    participantId: => ParticipantId,
    reference: ParticipantReference,
    override protected val consoleEnvironment: ConsoleEnvironment,
    override protected val loggerFactory: NamedLoggerFactory,
) extends PartiesAdministrationGroup(reference, consoleEnvironment)
    with FeatureFlagFilter {

  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  @Help.Summary("List parties hosted by this participant")
  @Help.Description("""Inspect the parties hosted by this participant as used for synchronisation.
      |The response is built from the timestamped topology transactions of each synchronizer, excluding the
      |authorized store of the given node. The search will include all hosted parties and is equivalent
      |to running the `list` method using the participant id of the invoking participant.
      |
      filterParty: Filter by parties starting with the given string.
      filterSynchronizerId: Filter by synchronizers whose id starts with the given string.
      asOf: Optional timestamp to inspect the topology state at a given point in time.
      limit: How many items to return (defaults to canton.parameters.console.default-limit)

      Example: participant1.parties.hosted(filterParty="alice")""")
  def hosted(
      filterParty: String = "",
      synchronizerIds: Set[SynchronizerId] = Set.empty,
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListPartiesResult] =
    list(
      filterParty,
      filterParticipant = participantId.filterString,
      synchronizerIds = synchronizerIds,
      asOf = asOf,
      limit = limit,
    )

  @Help.Summary("Find a party from a filter string")
  @Help.Description(
    """Will search for all parties that match this filter string. If it finds exactly one party, it
      |will return that one. Otherwise, the function will throw."""
  )
  def find(filterParty: String): PartyId =
    list(filterParty).map(_.party).distinct.toList match {
      case one :: Nil => one
      case Nil => throw new IllegalArgumentException(s"No party matching $filterParty")
      case more =>
        throw new IllegalArgumentException(s"Multiple parties match $filterParty: $more")
    }

  @Help.Summary("Enable/add party to participant")
  @Help.Description("""This function registers a new party on a synchronizer with the current participant within the
      |participants namespace. The function fails if the participant does not have appropriate signing keys
      |to issue the corresponding PartyToParticipant topology transaction, or if the participant is not connected to any
      |synchronizers.
      |The synchronizer parameter does not have to be specified if the participant is connected only to one synchronizer.
      |If the participant is connected to multiple synchronizers, the party needs to be enabled on each synchronizer explicitly.
      |Additionally, a sequence of additional participants can be added to be synchronized to
      |ensure that the party is known to these participants as well before the function terminates.
      |""")
  def enable(
      name: String,
      namespace: Namespace = participantId.namespace,
      synchronizer: Option[SynchronizerAlias] = None,
      synchronizeParticipants: Seq[ParticipantReference] = consoleEnvironment.participants.all,
      synchronize: Option[config.NonNegativeDuration] = Some(
        consoleEnvironment.commandTimeouts.unbounded
      ),
  ): PartyId =
    consoleEnvironment.run {
      ConsoleCommandResult.fromEither {
        for {
          // assert that name is valid ParticipantId
          _ <- LedgerParticipantId.fromString(name)
          partyId <- UniqueIdentifier.create(name, namespace).map(PartyId(_))
          // find the synchronizer id
          synchronizerId <- lookupOrDetectSynchronizerId(synchronizer)
          _ <- runPartyCommand(
            partyId,
            synchronizerId,
            synchronize,
          ).toEither

          _ <- Applicative[Either[String, *]].whenA(synchronize.nonEmpty)(
            PartiesAdministration.Allocation.waitForPartyKnown(
              partyId = partyId,
              hostingParticipant = reference,
              synchronizeParticipants = synchronizeParticipants,
              synchronizerId = synchronizerId.logical,
            )(consoleEnvironment)
          )
        } yield partyId
      }
    }

  private[canton] object external {

    private implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext
    private implicit val tc: TraceContext = TraceContext.empty
    private implicit val ce: ConsoleEnvironment = consoleEnvironment

    /** Enable an external party hosted on `reference` with confirmation rights.
      * @param name
      *   Name of the party to be enabled
      * @param synchronizer
      *   Synchronizer
      * @param synchronizeParticipants
      *   Participants that need to see activation of the party
      */
    @VisibleForTesting // Ensures external parties are created only in tests
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

    /** Generate the party id and namespace transaction for a centralized namespace party. Creates
      * the namespace key in the global crypto store.
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

    /** Generate the party id and namespace transaction for a decentralized namespace party from a
      * set of existing namespaces. The namespaces must already exist and be authorized in the
      * topology of the target synchronizer.
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
      *   Threshold of the decentralized namespace. Only used when decentralizedNamespaceOwners is
      *   non empty.
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
        ExternalParty(partyId, protocolSigningKeys.map(_.fingerprint)),
      )

    /** Enable an existing external party hosted on `reference` with confirmation rights. Unlike
      * `enable`, this command assumes the external party already exists on a different
      * synchronizer.
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
    def onboarding_transactions_for_existing(
        party: ExternalParty,
        synchronizer: SynchronizerAlias,
        additionalConfirming: Seq[ParticipantId] = Seq.empty,
        observing: Seq[ParticipantId] = Seq.empty,
        confirmationThreshold: PositiveInt = PositiveInt.one,
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
          protocolSigningKeys = party.signingFingerprints,
          protocolVersion = protocolVersion,
          namespaceTx = namespaceDelegationTx,
          partyToKeyTx = partyToKeyTx,
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

  private def runPartyCommand(
      partyId: PartyId,
      synchronizerId: PhysicalSynchronizerId,
      synchronize: Option[config.NonNegativeDuration],
  ): ConsoleCommandResult[SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant]] = {
    // determine the next serial
    val nextSerial = reference.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = partyId.filterString)
      .maxByOption(_.context.serial)
      .map(_.context.serial.increment)

    reference
      .adminCommand(
        TopologyAdminCommands.Write.Propose(
          mapping = PartyToParticipant.create(
            partyId,
            PositiveInt.one,
            Seq(
              HostingParticipant(
                participantId,
                ParticipantPermission.Submission,
              )
            ),
          ),
          // let the topology service determine the appropriate keys to use
          signedBy = Seq.empty,
          serial = nextSerial,
          store = synchronizerId,
          mustFullyAuthorize = true,
          change = TopologyChangeOp.Replace,
          forceChanges = ForceFlags.none,
          waitToBecomeEffective = synchronize,
        )
      )
  }

  @Help.Summary("Disable party on participant")
  def disable(
      party: PartyId,
      forceFlags: ForceFlags = ForceFlags.none,
      synchronizer: Option[SynchronizerAlias] = None,
  ): Unit = {
    val synchronizerId = consoleEnvironment.runE(lookupOrDetectSynchronizerId(synchronizer))
    reference.topology.party_to_participant_mappings
      .propose_delta(
        party,
        removes = List(this.participantId),
        forceFlags = forceFlags,
        store = synchronizerId,
      )
      .discard
  }

  @Help.Summary("Add a previously existing party to the local participant", FeatureFlag.Preview)
  @Help.Description(
    """Initiate adding a previously existing party to this participant on the specified synchronizer.
      |Performs some checks synchronously and then initiates party replication asynchronously. The returned `addPartyRequestId`
      |parameter allows identifying asynchronous progress and errors."""
  )
  def add_party_async(
      party: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipant: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  ): String = check(FeatureFlag.Preview) {
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.AddPartyAsync(
          party,
          synchronizerId,
          sourceParticipant,
          serial,
          participantPermission,
        )
      )
    }
  }

  @Help.Summary("Obtain status on a pending `add_party_async` call", FeatureFlag.Preview)
  @Help.Description(
    """Retrieve status information on a party previously added via the `add_party_async` endpoint
      |by specifying the previously returned `addPartyRequestId` parameter."""
  )
  def get_add_party_status(addPartyRequestId: String): AddPartyStatus = check(FeatureFlag.Preview) {
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.GetAddPartyStatus(addPartyRequestId)
      )
    }
  }

  @Help.Summary("Finds a party's highest activation offset.")
  @Help.Description(
    """This command locates the highest ledger offset where a party's activation matches
      |specified criteria.
      |
      |It searches the ledger for topology transactions, sequenced by the given synchronizer
      |(`synchronizerId`), that result in the party (`partyId`) being newly hosted on the
      |participant (`participantId`). An optional `validFrom` timestamp filters the topology
      |transactions for their effective time.
      |
      |The ledger search occurs within the specified offset range, targeting a specific number
      |of topology transactions (`completeAfter`).
      |
      |The search begins at the ledger start if `beginOffsetExclusive` is default. If the
      |participant was pruned and `beginOffsetExclusive` is below the pruning offset, a
      |`NOT_FOUND` error occurs. Use an `beginOffsetExclusive` near, but before, the desired
      |topology transactions.
      |
      |If `endOffsetInclusive` is not set (`None`), the search continues until `completeAfter`
      |number of transactions are found or the `timeout` expires. Otherwise, the ledger search
      |ends at the specified offset.
      |
      |This command is useful for creating ACS snapshots with `export_acs`, which requires the
      |party activation ledger offset.
      |
      |
      |The arguments are:
      |- partyId: The party to find activations for.
      |- participantId: The participant hosting the new party.
      |- synchronizerId: The synchronizer sequencing the activations.
      |- validFrom: The activation's effective time (default: None).
      |- beginOffsetExclusive: Starting ledger offset (default: 0).
      |- endOffsetInclusive: Ending ledger offset (default: None = trailing search).
      |- completeAfter: Number of transactions to find (default: Maximum = no limit).
      |- timeout: Search timeout (default: 1 minute).
      |"""
  )
  def find_party_max_activation_offset(
      partyId: PartyId,
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
      validFrom: Option[Instant] = None,
      beginOffsetExclusive: Long = 0L,
      endOffsetInclusive: Option[Long] = None,
      completeAfter: PositiveInt = PositiveInt.MaxValue,
      timeout: NonNegativeDuration = timeouts.bounded,
  ): NonNegativeLong = {
    val filter = TopologyTxFiltering.getTopologyFilter(
      partyId,
      participantId,
      synchronizerId,
      validFrom,
      AddedFilter,
    )(consoleEnvironment)

    findTopologyOffset(
      partyId,
      beginOffsetExclusive,
      endOffsetInclusive,
      completeAfter,
      timeout,
      filter,
    )
  }

  @Help.Summary("Finds a party's highest deactivation offset.")
  @Help.Description(
    """This command locates the highest ledger offset where a party's deactivation matches
      |specified criteria.
      |
      |It searches the ledger for topology transactions, sequenced by the given synchronizer
      |(`synchronizerId`), that result in the party (`partyId`) being revoked on the participant
      |(`participantId`). An optional `validFrom` timestamp filters the topology transactions
      |for their effective time.
      |
      |The ledger search occurs within the specified offset range, targeting a specific number
      |of topology transactions (`completeAfter`).
      |
      |The search begins at the ledger start if `beginOffsetExclusive` is default. If the
      |participant was pruned and `beginOffsetExclusive` is below the pruning offset, a
      |`NOT_FOUND` error occurs. Use an `beginOffsetExclusive` near, but before, the desired
      |topology transactions.
      |
      |If `endOffsetInclusive` is not set (`None`), the search continues until `completeAfter`
      |number of transactions are found or the `timeout` expires. Otherwise, the ledger search
      |ends at the specified offset.
      |
      |This command is useful for finding active contracts at the ledger offset where a party
      |has been off-boarded from a participant.
      |
      |
      |The arguments are:
      |- partyId: The party to find deactivations for.
      |- participantId: The participant hosting the new party.
      |- synchronizerId: The synchronizer sequencing the deactivations.
      |- validFrom: The deactivation's effective time (default: None).
      |- beginOffsetExclusive: Starting ledger offset (default: 0).
      |- endOffsetInclusive: Ending ledger offset (default: None = trailing search).
      |- completeAfter: Number of transactions to find (default: Maximum = no limit).
      |- timeout: Search timeout (default: 1 minute).
      |"""
  )
  def find_party_max_deactivation_offset(
      partyId: PartyId,
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
      validFrom: Option[Instant] = None,
      beginOffsetExclusive: Long = 0L,
      endOffsetInclusive: Option[Long] = None,
      completeAfter: PositiveInt = PositiveInt.MaxValue,
      timeout: NonNegativeDuration = timeouts.bounded,
  ): NonNegativeLong = {
    val filter = TopologyTxFiltering.getTopologyFilter(
      partyId,
      participantId,
      synchronizerId,
      validFrom,
      RevokedFilter,
    )(consoleEnvironment)

    findTopologyOffset(
      partyId,
      beginOffsetExclusive,
      endOffsetInclusive,
      completeAfter,
      timeout,
      filter,
    )
  }

  private def findTopologyOffset(
      party: PartyId,
      beginOffsetExclusive: Long,
      endOffsetInclusive: Option[Long],
      completeAfter: PositiveInt,
      timeout: NonNegativeDuration,
      filter: UpdateWrapper => Boolean,
  ): NonNegativeLong = {
    val topologyTransactions: Seq[com.daml.ledger.api.v2.topology_transaction.TopologyTransaction] =
      reference.ledger_api.updates
        .topology_transactions(
          partyIds = Seq(party),
          completeAfter = completeAfter,
          timeout = timeout,
          beginOffsetExclusive = beginOffsetExclusive,
          endOffsetInclusive = endOffsetInclusive,
          resultFilter = filter,
        )
        .collect { case TopologyTransactionWrapper(topologyTransaction) => topologyTransaction }

    topologyTransactions
      .map(_.offset)
      .map(NonNegativeLong.tryCreate)
      .lastOption
      .getOrElse(
        consoleEnvironment.raiseError(
          "Offset not found in topology data. Possible causes: " +
            "1) No topology transaction exists (Solution: Initiate a new topology transaction). " +
            "2) Existing topology transactions do not match the specified search criteria. (Solution: Adjust search criteria). " +
            "3) The ledger has not yet processed the relevant topology transaction. (Solution: Retry after delay, ensuring the ledger (end) has advanced)."
        )
      )
  }

  @Help.Summary("Find highest ledger offset by timestamp.")
  @Help.Description(
    """This command attempts to find the highest ledger offset among all events belonging
      |to a synchronizer that have a record time before or at the given timestamp.
      |
      |Returns the highest ledger offset, or an error.
      |
      |Possible failure causes:
      |- The requested timestamp is too far in the past for which no events exist anymore.
      |- There are no events for the given synchronizer.
      |- Not all events have been processed fully and/or published to the Ledger API DB
      |  until the requested timestamp.
      |
      |Depending on the failure cause, this command can be tried to get a ledger offset.
      |For example, if not all events have been processed fully and/or published to the
      |Ledger API DB, a retry makes sense.
      |
      |The arguments are:
      |- synchronizerId: Restricts the query to a particular synchronizer.
      |- timestamp: A point in time.
      |- force: Defaults to false. If true, returns the highest currently known ledger offset
      |  with a record time before or at the given timestamp.
      |"""
  )
  def find_highest_offset_by_timestamp(
      synchronizerId: SynchronizerId,
      timestamp: Instant,
      force: Boolean = false,
  ): NonNegativeLong = consoleEnvironment.run {
    reference.adminCommand(
      ParticipantAdminCommands.PartyManagement
        .GetHighestOffsetByTimestamp(synchronizerId, timestamp, force)
    )
  }

  @Help.Summary(
    "Export active contracts for a given party to replicate it."
  )
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) for a given
      |party to facilitate its replication from a source to a target participant.
      |
      |It uses the party's most recent activation on the target participant to
      |determine the precise historical state of the ACS to export from the
      |source participant.
      |
      |"Activation" on the target participant means the new hosting arrangement
      |has been authorized by both the party itself and the target participant
      |via party-to-participant topology transactions.
      |
      |This command will fail if the party has not yet been activated on the
      |target participant.
      |
      |Upon successful completion, the command writes a GZIP-compressed ACS
      |snapshot file. This file should then be imported into the target participant's
      |ACS using the `import_party_acs` command.
      |
      |The arguments are:
      |- party: The party being replicated, it must already be active on the target participant.
      |- synchronizerId: Restricts the export to the given synchronizer.
      |- targetParticipantId: Unique identifier of the target participant where the party
      |                       will be replicated.
      |- beginOffsetExclusive: Exclusive ledger offset used as starting point fo find the party's
      |                        activation on the target participant.
      |- exportFilePath: The path denoting the file where the ACS snapshot will be stored.
      |- waitForActivationTimeout: The maximum duration the service will wait to find the topology
      |                            transaction that activates the party on the target participant.
      |- timeout: A timeout for this operation to complete.
      """
  )
  def export_party_acs(
      party: PartyId,
      synchronizerId: SynchronizerId,
      targetParticipantId: ParticipantId,
      beginOffsetExclusive: Long,
      exportFilePath: String = "canton-acs-export.gz",
      waitForActivationTimeout: Option[config.NonNegativeFiniteDuration] = Some(
        config.NonNegativeFiniteDuration.ofMinutes(2)
      ),
      timeout: config.NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    consoleEnvironment.run {
      val file = File(exportFilePath)
      val responseObserver = new FileStreamObserver[ExportPartyAcsResponse](file, _.chunk)

      def call: ConsoleCommandResult[Context.CancellableContext] =
        reference.adminCommand(
          ParticipantAdminCommands.PartyManagement.ExportPartyAcs(
            party,
            synchronizerId,
            targetParticipantId,
            NonNegativeLong.tryCreate(beginOffsetExclusive),
            waitForActivationTimeout,
            responseObserver,
          )
        )

      processResult(
        call,
        responseObserver.result,
        timeout,
        request = "exporting party acs",
        cleanupOnError = () => file.delete(),
      )
    }

  @Help.Summary(
    "Import active contracts from a snapshot file to replicate a party."
  )
  @Help.Description(
    """This command imports contracts from an Active Contract Set (ACS) snapshot
      |file into the participant's ACS. It expects the given ACS snapshot file to
      |be the result of a previous `export_party_acs` command invocation.
      |
      |The argument is:
      |- importFilePath: The path denoting the file from where the ACS snapshot will be read.
      |                  Defaults to "canton-acs-export.gz" when undefined.
      """
  )
  def import_party_acs(
      importFilePath: String = "canton-acs-export.gz"
  ): Unit =
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.ImportPartyAcs(
          ByteString.copyFrom(File(importFilePath).loadBytes)
        )
      )
    }

  @Help.Summary(
    "Finalize the party onboarding"
  )
  @Help.Description(
    """Finalizes a party's onboarding by having the target participant unilaterally remove
      |the onboarding flag on the party to participant topology mapping.
      |
      |The successful removal depends on the change history of the dynamic synchronizer's
      |confirmation response timeout and the mediator reaction timeout parameters.
      |
      |Returns a tuple with a boolean and a timestamp:
      |- True if the party onboarding was successfully finalized, false otherwise.
      |- If onboarding finalization failed, then the timestamp suggests the earliest time to retry
      |  the call.
      |
      |The arguments are:
      |- party: The party being onboarded, it must already be active on the target participant.
      |- synchronizerId: Restricts the party onboarding to the given synchronizer.
      |- targetParticipantId: Unique identifier of the target participant where the party
      |                       has been onboarded.
      |- beginOffsetExclusive: Exclusive ledger offset used as starting point fo find the party's
      |                        activation on the target participant.
      |- waitForActivationTimeout: The maximum duration the service will wait to find the topology
      |                            transaction that activates the party on the target participant.
      """
  )
  def complete_party_onboarding(
      party: PartyId,
      synchronizerId: SynchronizerId,
      targetParticipantId: ParticipantId,
      beginOffsetExclusive: Long,
      waitForActivationTimeout: Option[config.NonNegativeFiniteDuration] = Some(
        config.NonNegativeFiniteDuration.ofMinutes(2)
      ),
  ): (Boolean, Option[CantonTimestamp]) =
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.CompletePartyOnboarding(
          party,
          synchronizerId,
          targetParticipantId,
          NonNegativeLong.tryCreate(beginOffsetExclusive),
          waitForActivationTimeout,
        )
      )
    }

}

private[canton] object PartiesAdministration {
  object Allocation {

    /** Ensure a new party is known by some participants
      * @param partyId
      *   Party to be known
      * @param hostingParticipant
      *   The party hosting the patry
      * @param synchronizeParticipants
      *   All the participants that need to know the party
      * @param synchronizerId
      *   Synchronizer
      */
    def waitForPartyKnown(
        partyId: PartyId,
        hostingParticipant: ParticipantReference,
        synchronizeParticipants: Seq[ParticipantReference],
        synchronizerId: SynchronizerId,
    )(implicit consoleEnvironment: ConsoleEnvironment): Either[String, Unit] =
      for {
        _ <- retryE(
          hostingParticipant.ledger_api.parties.list().map(_.party).contains(partyId),
          show"The party $partyId never appeared on the ledger API server",
        )

        // Party is known on relevant participants
        otherParticipants = synchronizeParticipants.filter(
          _.synchronizers.is_connected(synchronizerId)
        )
        _ <- (hostingParticipant +: otherParticipants).traverse_(p =>
          waitForParty(
            partyId,
            synchronizerId,
            hostingParticipant = hostingParticipant.id,
            queriedParticipant = p,
          )
        )
      } yield ()

    private def synchronizersPartyIsRegisteredOn(
        hostingParticipant: ParticipantId,
        participant: ParticipantReference,
        partyId: PartyId,
    ): Set[SynchronizerId] =
      participant.parties
        .list(
          filterParty = partyId.filterString,
          filterParticipant = hostingParticipant.filterString,
        )
        .flatMap(_.participants.flatMap(_.synchronizers))
        .map(_.synchronizerId)
        .toSet

    private def waitForParty(
        partyId: PartyId,
        synchronizerId: SynchronizerId,
        hostingParticipant: ParticipantId,
        queriedParticipant: ParticipantReference,
    )(implicit consoleEnvironment: ConsoleEnvironment): Either[String, Unit] =
      retryE(
        synchronizersPartyIsRegisteredOn(hostingParticipant, queriedParticipant, partyId).contains(
          synchronizerId
        ),
        show"Party $partyId did not appear for $queriedParticipant on synchronizer $synchronizerId}",
      )
  }

  private def retryE(condition: => Boolean, message: => String)(implicit
      consoleEnvironment: ConsoleEnvironment
  ): Either[String, Unit] =
    AdminCommandRunner
      .retryUntilTrue(consoleEnvironment.commandTimeouts.ledgerCommand)(condition)
      .toEither
      .leftMap(_ => message)
}

private object TopologyTxFiltering {
  sealed trait AuthorizationFilterKind
  case object AddedFilter extends AuthorizationFilterKind
  case object RevokedFilter extends AuthorizationFilterKind

  def getTopologyFilter(
      partyId: PartyId,
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
      validFrom: Option[Instant],
      filterType: AuthorizationFilterKind,
  )(consoleEnvironment: ConsoleEnvironment): UpdateWrapper => Boolean = {
    def filterOnEffectiveTime(
        tx: com.daml.ledger.api.v2.topology_transaction.TopologyTransaction,
        recordTime: Option[Instant],
    ): Boolean =
      recordTime.forall { instant =>
        tx.recordTime match {
          case Some(ts) =>
            ProtoConverter.InstantConverter
              .fromProtoPrimitive(ts)
              .valueOr(err =>
                consoleEnvironment.raiseError(
                  s"Failed record time timestamp conversion for $ts: $err"
                )
              ) == instant
          case None => false
        }
      }

    def filter(wrapper: UpdateWrapper): Boolean =
      wrapper match {
        case TopologyTransactionWrapper(tx) =>
          synchronizerId.toProtoPrimitive == wrapper.synchronizerId &&
          tx.events.exists { tx =>
            filterType match {
              case AddedFilter =>
                val added = tx.getParticipantAuthorizationAdded
                added.partyId == partyId.toLf && added.participantId == participantId.toLf
              case RevokedFilter =>
                val revoked = tx.getParticipantAuthorizationRevoked
                revoked.partyId == partyId.toLf && revoked.participantId == participantId.toLf
            }
          } &&
          filterOnEffectiveTime(tx, validFrom)
        case _ => false
      }

    filter
  }
}
