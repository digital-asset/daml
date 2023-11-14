// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.{
  TopologyAdminCommands,
  TopologyAdminCommandsX,
}
import com.digitalasset.canton.admin.api.client.data.topologyx.*
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.console.{
  CommandErrors,
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReferenceCommon,
  InstanceReferenceX,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.BaseQueryX
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransactionsX, TimeQueryX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.reflect.ClassTag

trait InitNodeIdX extends ConsoleCommandGroup {

  @Help.Summary("Initialize the node with a unique identifier")
  @Help.Description("""Every node in Canton is identified using a unique identifier, which is composed
                      |of a user-chosen string and the fingerprint of a signing key. The signing key is the root key
                      |defining a so-called namespace, where the signing key has the ultimate control over
                      |issuing new identifiers.
                      |During initialisation, we have to pick such a unique identifier.
                      |By default, initialisation happens automatically, but it can be turned off by setting the auto-init
                      |option to false.
                      |Automatic node initialisation is usually turned off to preserve the identity of a participant or domain
                      |node (during major version upgrades) or if the topology transactions are managed through
                      |a different topology manager than the one integrated into this node.""")
  def init_id(identifier: UniqueIdentifier): Unit =
    consoleEnvironment.run {
      runner.adminCommand(TopologyAdminCommandsX.Init.InitId(identifier.toProtoPrimitive))
    }

}

class TopologyAdministrationGroupX(
    instance: InstanceReferenceX,
    topologyQueueStatus: => Option[TopologyQueueStatus],
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends TopologyAdministrationGroupCommon(
      instance,
      topologyQueueStatus,
      consoleEnvironment,
      loggerFactory,
    )
    with InitNodeIdX
    with Helpful
    with FeatureFlagFilter {

  import runner.*

  override protected def getIdCommand(): ConsoleCommandResult[UniqueIdentifier] =
    runner.adminCommand(TopologyAdminCommandsX.Init.GetId())

  @Help.Summary("Inspect all topology transactions at once")
  @Help.Group("All Transactions")
  object transactions {

    @Help.Summary("Downloads the node's topology identity transactions")
    @Help.Description(
      "The node's identity is defined by topology transactions of type NamespaceDelegationX and OwnerToKeyMappingX."
    )
    def identity_transactions()
        : Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]] = {
      val txs = instance.topology.transactions.list()
      txs.result
        .flatMap(tx =>
          tx.transaction
            .selectMapping[NamespaceDelegationX]
            .orElse(tx.transaction.selectMapping[OwnerToKeyMappingX])
        )
        .filter(_.transaction.mapping.namespace == instance.id.uid.namespace)
    }

    @Help.Summary("Upload signed topology transaction")
    @Help.Description(
      """Topology transactions can be issued with any topology manager. In some cases, such
      |transactions need to be copied manually between nodes. This function allows for
      |uploading previously exported topology transaction into the authorized store (which is
      |the name of the topology managers transaction store."""
    )
    def load_serialized(bytes: ByteString): Unit =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.AddSignedTopologyTransaction(bytes)
        )
      }

    def load(transactions: Seq[GenericSignedTopologyTransactionX], store: String): Unit =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.AddTransactions(transactions, store)
        )
      }

    def sign(
        transactions: Seq[GenericSignedTopologyTransactionX],
        signedBy: Seq[Fingerprint] = Seq(instance.id.uid.namespace.fingerprint),
    ): Seq[GenericSignedTopologyTransactionX] =
      consoleEnvironment.run {
        adminCommand(TopologyAdminCommandsX.Write.SignTransactions(transactions, signedBy))
      }

    def authorize[M <: TopologyMappingX: ClassTag](
        txHash: TxHash,
        mustBeFullyAuthorized: Boolean,
        store: String,
        signedBy: Seq[Fingerprint] = Seq(instance.id.uid.namespace.fingerprint),
    ): SignedTopologyTransactionX[TopologyChangeOpX, M] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Authorize(
            txHash.hash.toHexString,
            mustFullyAuthorize = mustBeFullyAuthorized,
            signedBy = signedBy,
            store = store,
          )
        )
      }

    @Help.Summary("List all transaction")
    def list(
        filterStore: String = AuthorizedStore.filterName,
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterAuthorizedKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX] = {
      consoleEnvironment
        .run {
          adminCommand(
            TopologyAdminCommandsX.Read.ListAll(
              BaseQueryX(
                filterStore,
                includeProposals,
                timeQuery,
                operation,
                filterSigningKey = filterAuthorizedKey.map(_.toProtoPrimitive).getOrElse(""),
                protocolVersion.map(ProtocolVersion.tryCreate),
              )
            )
          )
        }
    }

    @Help.Summary("Manage topology transaction purging", FeatureFlag.Preview)
    @Help.Group("Purge Topology Transactions")
    object purge extends Helpful {
      def list(
          filterStore: String = "",
          includeProposals: Boolean = false,
          timeQuery: TimeQueryX = TimeQueryX.HeadState,
          operation: Option[TopologyChangeOpX] = None,
          filterDomain: String = "",
          filterSigningKey: String = "",
          protocolVersion: Option[String] = None,
      ): Seq[ListPurgeTopologyTransactionXResult] = consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Read.PurgeTopologyTransactionX(
            BaseQueryX(
              filterStore,
              includeProposals,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterDomain,
          )
        )
      }

      // TODO(#15236): implement write service for purging
    }
  }

  object domain_bootstrap {

    @Help.Summary(
      """Creates and returns proposals of topology transactions to bootstrap a domain, specifically
        |DomainParametersStateX, SequencerDomainStateX, and MediatorDomainStateX.""".stripMargin
    )
    def generate_genesis_topology(
        domainId: DomainId,
        domainOwners: Seq[Member],
        sequencers: Seq[SequencerId],
        mediators: Seq[MediatorId],
    ): Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]] = {
      val isDomainOwner = domainOwners.contains(instance.id)
      require(isDomainOwner, s"Only domain owners should call $functionFullName.")

      val thisNodeRootKey = Some(instance.id.uid.namespace.fingerprint)

      // create and sign the initial domain parameters
      val domainParameterState =
        instance.topology.domain_parameters.propose(
          domainId,
          DynamicDomainParameters
            .initialXValues(
              consoleEnvironment.environment.clock,
              ProtocolVersion.CNTestNet,
            ), // TODO(#15358): Use `ProtocolVersion.latest`
          signedBy = thisNodeRootKey,
          store = Some(AuthorizedStore.filterName),
        )

      val mediatorState =
        instance.topology.mediators.propose(
          domainId,
          threshold = PositiveInt.one,
          active = mediators,
          signedBy = thisNodeRootKey,
          store = Some(AuthorizedStore.filterName),
        )

      val sequencerState =
        instance.topology.sequencers.propose(
          domainId,
          threshold = PositiveInt.one,
          active = sequencers,
          signedBy = thisNodeRootKey,
          store = Some(AuthorizedStore.filterName),
        )

      Seq(domainParameterState, sequencerState, mediatorState)
    }
  }

  @Help.Summary("Manage unionspaces")
  @Help.Group("Unionspaces")
  object unionspaces extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterNamespace: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListUnionspaceDefinitionResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListUnionspaceDefinition(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterNamespace,
        )
      )
    }

    def propose(
        owners: Set[Fingerprint],
        threshold: PositiveInt,
        store: String,
        // TODO(#14056) don't use the instance's root namespace key by default.
        //  let the grpc service figure out the right key to use, once that's implemented
        signedBy: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
        serial: Option[PositiveInt] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, UnionspaceDefinitionX] =
      consoleEnvironment.run {
        NonEmpty
          .from(owners) match {
          case Some(ownersNE) =>
            adminCommand(
              {
                TopologyAdminCommandsX.Write.Propose(
                  UnionspaceDefinitionX
                    .create(
                      UnionspaceDefinitionX.computeNamespace(owners.map(Namespace(_))),
                      threshold,
                      ownersNE.map(Namespace(_)),
                    ),
                  signedBy = signedBy.toList,
                  serial = serial,
                  change = TopologyChangeOpX.Replace,
                  mustFullyAuthorize = false,
                  store = store,
                )
              }
            )
          case None =>
            CommandErrors.GenericCommandError("Proposed unionspace needs at least one owner")
        }
      }

    def join(
        unionspace: Fingerprint,
        owner: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
    ): GenericSignedTopologyTransactionX = {
      ???
    }

    def leave(
        unionspace: Fingerprint,
        owner: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
    ): ByteString = {
      ByteString.EMPTY
    }
  }

  @Help.Summary("Manage namespace delegations")
  @Help.Group("Namespace delegations")
  object namespace_delegations extends Helpful {

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterNamespace: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListNamespaceDelegationResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListNamespaceDelegation(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterNamespace,
          filterTargetKey,
        )
      )
    }
  }

  @Help.Summary("Manage identifier delegations")
  @Help.Group("Identifier delegations")
  object identifier_delegations extends Helpful {

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterUid: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListIdentifierDelegationResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListIdentifierDelegation(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
          filterTargetKey,
        )
      )
    }
  }

  // TODO(#14057) complete @Help.Description's (by adapting TopologyAdministrationGroup-non-X descriptions)
  @Help.Summary("Manage owner to key mappings")
  @Help.Group("Owner to key mappings")
  object owner_to_key_mappings
      extends OwnerToKeyMappingsGroup(consoleEnvironment.commandTimeouts)
      with Helpful {

    @Help.Summary("List owner to key mapping transactions")
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterKeyOwnerType: Option[KeyOwnerCode] = None,
        filterKeyOwnerUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListOwnerToKeyMappingResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Read.ListOwnerToKeyMapping(
            BaseQueryX(
              filterStore,
              includeProposals,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterKeyOwnerType,
            filterKeyOwnerUid,
          )
        )
      }

    // TODO(#15237) implement authorize and rotate keys

    def rotate_key(
        owner: KeyOwner,
        currentKey: PublicKey,
        newKey: PublicKey,
    ): Unit =
      throw new IllegalArgumentException(
        s"For this node use `rotate_key` where you specify the node instance"
      )

    @Help.Summary("Rotate the key for an owner to key mapping")
    // TODO(#15237): Implement write service
    def rotate_key(
        nodeInstance: InstanceReferenceCommon,
        owner: KeyOwner,
        currentKey: PublicKey,
        newKey: PublicKey,
    ): Unit = ???
  }

  @Help.Summary("Manage party to participant mappings")
  @Help.Group("Party to participant mappings")
  object party_to_participant_mappings extends Helpful {
    // TODO(#15238): implement properly
    def proposeDelta(
        party: PartyId,
        adds: List[(ParticipantId, ParticipantPermissionX)] = Nil,
        removes: List[ParticipantId] = Nil,
        domainId: Option[DomainId] = None,
        serial: Option[PositiveInt] = None,
    ) = {

      val currentO = list(
        filterStore = "Authorized",
        filterParty = party.filterString,
      ).groupBy(_.item.domainId).get(domainId).flatMap(_.maxByOption(_.context.serial))

      val (newDomainId, existingPermissions, newSerial) = currentO match {
        case Some(current) =>
          val newDomainId = current.item.domainId

          val currentPermissions =
            current.item.participants.map(p => p.participantId -> p.permission).toMap

          (newDomainId, currentPermissions, Some(current.context.serial.increment))

        case None =>
          (domainId, Map.empty[ParticipantId, ParticipantPermissionX], serial)
      }

      val newPermissions = new PartyToParticipantComputations(loggerFactory)
        .computeNewPermissions(
          existingPermissions = existingPermissions,
          adds = adds,
          removes = removes,
        )(TraceContext.empty)
        .valueOr(err => throw new RuntimeException(err))

      propose(
        party = party,
        newParticipants = newPermissions.toSeq,
        domainId = newDomainId,
        serial = newSerial,
      )
    }

    // TODO(#15238): implement write service properly
    def propose(
        party: PartyId,
        newParticipants: Seq[(ParticipantId, ParticipantPermissionX)],
        threshold: PositiveInt = PositiveInt.one,
        domainId: Option[DomainId] = None,
        serial: Option[PositiveInt] = None,
        groupAddressing: Boolean = false,
        mustFullyAuthorize: Boolean = false,
        store: String = AuthorizedStore.filterName,
    ): SignedTopologyTransactionX[TopologyChangeOpX, PartyToParticipantX] = {
      val op = NonEmpty.from(newParticipants) match {
        case Some(_) => TopologyChangeOpX.Replace
        case None => TopologyChangeOpX.Remove
      }

      val command = TopologyAdminCommandsX.Write.Propose(
        mapping = PartyToParticipantX(
          partyId = party,
          domainId = domainId,
          threshold = threshold,
          participants = newParticipants.map((HostingParticipant.apply _) tupled),
          groupAddressing = groupAddressing,
        ),
        signedBy = Seq(instance.id.uid.namespace.fingerprint),
        serial = serial,
        change = op,
        mustFullyAuthorize = mustFullyAuthorize,
        store = store,
      )

      consoleEnvironment.run(adminCommand(command))
    }

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterParty: String = "",
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyToParticipantResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListPartyToParticipant(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParty,
          filterParticipant,
        )
      )
    }
  }

  @Help.Summary("Manage domain trust certificates")
  @Help.Group("Domain trust certificates")
  object domain_trust_certificates extends Helpful {
    // TODO(#15240): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        // TODO(#14048) should be filterDomain and filterParticipant
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListDomainTrustCertificateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListDomainTrustCertificate(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }

    // TODO(#14057) document console command
    def active(domainId: DomainId, participantId: ParticipantId): Boolean =
      list(filterStore = domainId.filterString).exists { x =>
        x.item.domainId == domainId && x.item.participantId == participantId
      }

  }

  @Help.Summary("Inspect participant domain states")
  @Help.Group("Participant Domain States")
  object participant_domain_permissions extends Helpful {
    // TODO(#15241): implement write service properly
    def authorize(
        domainId: DomainId,
        participant: ParticipantId,
        permission: ParticipantPermissionX,
        trustLevel: TrustLevelX = TrustLevelX.Ordinary,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        store: Option[String] = None,
    ): ConsoleCommandResult[
      SignedTopologyTransactionX[TopologyChangeOpX, ParticipantDomainPermissionX]
    ] = {
      val cmd = TopologyAdminCommandsX.Write.Propose(
        mapping = ParticipantDomainPermissionX(
          domainId = domainId,
          participantId = participant,
          permission = permission,
          trustLevel = trustLevel,
          limits = None,
          loginAfter = None,
        ),
        signedBy = Seq(instance.id.uid.namespace.fingerprint),
        serial = None,
        store = store.getOrElse(domainId.filterString),
      )

      synchronisation.run(synchronize)(adminCommand(cmd))
    }

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListParticipantDomainPermissionResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListParticipantDomainPermission(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }
  }

  @Help.Summary("Inspect participant domain states")
  @Help.Group("Participant Domain States")
  object participant_domain_states extends Helpful {
    @Help.Summary("Returns true if the given participant is currently active on the given domain")
    @Help.Description(
      """Active means that the participant has been granted at least observation rights on the domain
         |and that the participant has registered a domain trust certificate"""
    )
    def active(domainId: DomainId, participantId: ParticipantId): Boolean = {
      // TODO(#14048) Should we check the other side (domain accepts participant)?
      domain_trust_certificates.active(domainId, participantId)
    }
  }

  @Help.Summary("Manage traffic control")
  @Help.Group("Member traffic control")
  object traffic_control {
    @Help.Summary("List traffic control topology transactions.")
    def list(
        filterMember: String = instance.id.filterString,
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListTrafficStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListTrafficControlState(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterMember = filterMember,
        )
      )
    }

    @Help.Summary("Top up traffic for this node")
    @Help.Description(
      """Use this command to update the new total traffic limit for the node.
         The top up will have to be authorized by the domain to be accepted.
         The newTotalTrafficAmount must be strictly increasing top up after top up."""
    )
    def top_up(
        domainId: DomainId,
        newTotalTrafficAmount: PositiveLong,
        member: Member = instance.id.member,
        serial: Option[PositiveInt] = None,
        signedBy: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
    ): SignedTopologyTransactionX[TopologyChangeOpX, TrafficControlStateX] = {
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            TrafficControlStateX
              .create(
                domainId,
                member,
                newTotalTrafficAmount,
              ),
            signedBy = signedBy.toList,
            serial = serial,
            change = TopologyChangeOpX.Replace,
            mustFullyAuthorize = true,
            store = domainId.filterString,
          )
        )
      }
    }
  }

  @Help.Summary("Manage party hosting limits")
  @Help.Group("Party hosting limits")
  object party_hosting_limits extends Helpful {
    // TODO(#15242): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyHostingLimitsResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListPartyHostingLimits(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }
  }

  @Help.Summary("Manage package vettings")
  @Help.Group("Vetted Packages")
  object vetted_packages extends Helpful {
    // TODO(#15243): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListVettedPackagesResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListVettedPackages(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParticipant,
        )
      )
    }
  }

  @Help.Summary("Manage authority-of mappings")
  @Help.Group("Authority-of mappings")
  object authority_of extends Helpful {
    def propose(
        partyId: PartyId,
        threshold: Int,
        parties: Seq[PartyId],
        domainId: Option[DomainId] = None,
        // TODO(#14056) don't use the instance's root namespace key by default.
        //  let the grpc service figure out the right key to use, once that's implemented
        signedBy: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
        serial: Option[PositiveInt] = None,
        store: String = AuthorizedStore.filterName,
    ): SignedTopologyTransactionX[TopologyChangeOpX, AuthorityOfX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            AuthorityOfX(
              partyId,
              domainId,
              PositiveInt.tryCreate(threshold),
              parties,
            ),
            signedBy = signedBy.toList,
            serial = serial,
            store = store,
          )
        )
      }

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterParty: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListAuthorityOfResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListAuthorityOf(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParty,
        )
      )
    }
  }

  @Help.Summary("Inspect mediator domain state")
  @Help.Group("Mediator Domain State")
  object mediators extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterDomain: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListMediatorDomainStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.MediatorDomainState(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterDomain,
        )
      )
    }

    def propose(
        domainId: DomainId,
        threshold: PositiveInt,
        active: Seq[MediatorId],
        passive: Seq[MediatorId] = Seq.empty,
        group: NonNegativeInt = NonNegativeInt.zero,
        // TODO(#14056) don't use the instance's root namespace key by default.
        //  let the grpc service figure out the right key to use, once that's implemented
        signedBy: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
        serial: Option[PositiveInt] = None,
        store: Option[String] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, MediatorDomainStateX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            mapping = MediatorDomainStateX
              .create(domainId, group, threshold, active, passive),
            signedBy = signedBy.toList,
            serial = serial,
            change = TopologyChangeOpX.Replace,
            mustFullyAuthorize = false,
            store = store.getOrElse(domainId.filterString),
          )
        )
      }
  }

  @Help.Summary("Inspect sequencer domain state")
  @Help.Group("Sequencer Domain State")
  object sequencers extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterDomain: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListSequencerDomainStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.SequencerDomainState(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterDomain,
        )
      )
    }

    def propose(
        domainId: DomainId,
        threshold: PositiveInt,
        active: Seq[SequencerId],
        passive: Seq[SequencerId] = Seq.empty,
        // TODO(#14056) don't use the instance's root namespace key by default.
        //  let the grpc service figure out the right key to use, once that's implemented
        signedBy: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
        serial: Option[PositiveInt] = None,
        store: Option[String] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, SequencerDomainStateX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            mapping = SequencerDomainStateX.create(domainId, threshold, active, passive),
            signedBy = signedBy.toList,
            serial = serial,
            change = TopologyChangeOpX.Replace,
            mustFullyAuthorize = false,
            store = store.getOrElse(domainId.filterString),
          )
        )
      }
  }

  @Help.Summary("Manage domain parameters state", FeatureFlag.Preview)
  @Help.Group("Domain Parameters State")
  object domain_parameters extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterDomain: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListDomainParametersStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.DomainParametersState(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterDomain,
        )
      )
    }

    def propose(
        domain: DomainId,
        parameters: DynamicDomainParameters,
        // TODO(#14056) don't use the instance's root namespace key by default.
        //  let the grpc service figure out the right key to use, once that's implemented
        signedBy: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
        serial: Option[PositiveInt] = None,
        store: Option[String] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, DomainParametersStateX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            // TODO(#14058) maybe don't just take default values for dynamic parameters
            DomainParametersStateX(
              domain,
              parameters,
            ),
            signedBy.toList,
            serial = serial,
            mustFullyAuthorize = false,
            store = store.getOrElse(domain.filterString),
          )
        )
      }
  }

  @Help.Summary("Inspect topology stores")
  @Help.Group("Topology stores")
  object stores extends Helpful {
    @Help.Summary("List available topology stores")
    def list(): Seq[String] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Read.ListStores()
        )
      }
  }
}
