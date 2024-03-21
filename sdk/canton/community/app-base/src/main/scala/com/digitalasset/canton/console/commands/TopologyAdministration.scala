// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.option.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.admin.api.client.commands.TopologyAdminCommands
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  Help,
  Helpful,
  InstanceReferenceCommon,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.DynamicDomainParameters as DynamicDomainParametersInternal
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.BaseQuery
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransactions, TimeQuery}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicReference

abstract class TopologyAdministrationGroupCommon(
    instance: InstanceReferenceCommon,
    topologyQueueStatus: => Option[TopologyQueueStatus],
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends Helpful {

  protected val runner: AdminCommandRunner = instance

  def owner_to_key_mappings: OwnerToKeyMappingsGroup

  // small cache to avoid repetitive calls to fetchId (as the id is immutable once set)
  protected val idCache =
    new AtomicReference[Option[UniqueIdentifier]](None)

  private[console] def clearCache(): Unit = {
    idCache.set(None)
  }

  protected def getIdCommand(): ConsoleCommandResult[UniqueIdentifier]

  private[console] def idHelper[T](
      apply: UniqueIdentifier => T
  ): T = {
    apply(idCache.get() match {
      case Some(v) => v
      case None =>
        val r = consoleEnvironment.run {
          getIdCommand()
        }
        idCache.set(Some(r))
        r
    })
  }

  @Help.Summary("Topology synchronisation helpers", FeatureFlag.Preview)
  @Help.Group("Synchronisation Helpers")
  object synchronisation {

    @Help.Summary("Check if the topology processing of a node is idle")
    @Help.Description("""Topology transactions pass through a set of queues before becoming effective on a domain.
        |This function allows to check if all the queues are empty.
        |While both domain and participant nodes support similar queues, there is some ambiguity around
        |the participant queues. While the domain does really know about all in-flight transactions at any
        |point in time, a participant won't know about the state of any transaction that is currently being processed
        |by the domain topology dispatcher.""")
    def is_idle(): Boolean =
      topologyQueueStatus
        .forall(_.isIdle) // report un-initialised as idle to not break manual init process

    @Help.Summary("Wait until the topology processing of a node is idle")
    @Help.Description("""This function waits until the `is_idle()` function returns true.""")
    def await_idle(
        timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded
    ): Unit =
      ConsoleMacros.utils.retry_until_true(timeout)(
        is_idle(),
        s"topology queue status never became idle ${topologyQueueStatus} after ${timeout}",
      )

    /** run a topology change command synchronized and wait until the node becomes idle again */
    private[console] def run[T](timeout: Option[NonNegativeDuration])(func: => T): T = {
      val ret = func
      timeout.foreach(tm => ConsoleMacros.utils.synchronize_topology(Some(tm))(consoleEnvironment))
      ret
    }
  }

}

/** OwnerToKeyMappingsGroup to parameterize by different TopologyChangeOp/X
  */
abstract class OwnerToKeyMappingsGroup(
    commandTimeouts: ConsoleCommandTimeout
) {
  def rotate_key(
      owner: KeyOwner,
      currentKey: PublicKey,
      newKey: PublicKey,
  ): Unit

  def rotate_key(
      nodeInstance: InstanceReferenceCommon,
      owner: KeyOwner,
      currentKey: PublicKey,
      newKey: PublicKey,
  ): Unit
}

trait InitNodeId extends ConsoleCommandGroup {

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
  def init_id(identifier: Identifier, fingerprint: Fingerprint): UniqueIdentifier =
    consoleEnvironment.run {
      runner.adminCommand(TopologyAdminCommands.Init.InitId(identifier.unwrap, fingerprint.unwrap))
    }

}

class TopologyAdministrationGroup(
    instance: InstanceReferenceCommon,
    topologyQueueStatus: => Option[TopologyQueueStatus],
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends TopologyAdministrationGroupCommon(
      instance,
      topologyQueueStatus,
      consoleEnvironment,
      loggerFactory,
    )
    with InitNodeId {

  import runner.*

  override protected def getIdCommand(): ConsoleCommandResult[UniqueIdentifier] =
    runner.adminCommand(TopologyAdminCommands.Init.GetId())

  @Help.Summary("Upload signed topology transaction")
  @Help.Description(
    """Topology transactions can be issued with any topology manager. In some cases, such
      |transactions need to be copied manually between nodes. This function allows for
      |uploading previously exported topology transaction into the authorized store (which is
      |the name of the topology managers transaction store."""
  )
  def load_transaction(bytes: ByteString): Unit =
    consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Write.AddSignedTopologyTransaction(bytes)
      )
    }

  @Help.Summary("Inspect topology stores")
  @Help.Group("Topology stores")
  object stores extends Helpful {
    @Help.Summary("List available topology stores")
    @Help.Description("""Topology transactions are stored in these stores. There are the following stores:
        |
        |"Authorized" - The authorized store is the store of a topology manager. Updates to the topology state are made
        | by adding new transactions to the "Authorized" store. Both the participant and the domain nodes topology manager
        | have such a store.
        | A participant node will distribute all the content in the Authorized store to the domains it is connected to.
        | The domain node will distribute the content of the Authorized store through the sequencer to the domain members
        | in order to create the authoritative topology state on a domain (which is stored in the store named using the domain-id),
        | such that every domain member will have the same view on the topology state on a particular domain.
        |
        |"<domain-id>" - The domain store is the authorized topology state on a domain. A participant has one store for each
        | domain it is connected to. The domain has exactly one store with its domain-id.
        |
        |"Requested" - A domain can be configured such that when participant tries to register a topology transaction with
        | the domain, the transaction is placed into the "Requested" store such that it can be analysed and processed with
        | user defined process.
        |""")
    def list(): Seq[String] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListStores()
        )
      }
  }

  @Help.Summary("Manage namespace delegations")
  @Help.Group("Namespace delegations")
  object namespace_delegations extends Helpful {

    @Help.Summary("Change namespace delegation")
    @Help.Description(
      """Delegates the authority to authorize topology transactions in a certain namespace to a certain
      |key. The keys are referred to using their fingerprints. They need to be either locally generated or have been
      |previously imported.
    ops: Either Add or Remove the delegation.
    namespace: The namespace whose authorization authority is delegated.
    signedBy: Optional fingerprint of the authorizing key. The authorizing key needs to be either the authorizedKey
              for root certificates. Otherwise, the signedBy key needs to refer to a previously authorized key, which
              means that we use the signedBy key to refer to a locally available CA.
    authorizedKey: Fingerprint of the key to be authorized. If signedBy equals authorizedKey, then this transaction
                   corresponds to a self-signed root certificate. If the keys differ, then we get an intermediate CA.
    isRootDelegation: If set to true (default = false), the authorized key will be allowed to issue NamespaceDelegations.
    synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node"""
    )
    def authorize(
        ops: TopologyChangeOp,
        namespace: Fingerprint,
        authorizedKey: Fingerprint,
        isRootDelegation: Boolean = false,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // intentionally not documented force flag, as it is dangerous
        force: Boolean = false,
    ): ByteString =
      synchronisation.run(synchronize)(consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AuthorizeNamespaceDelegation(
              ops,
              signedBy,
              namespace,
              authorizedKey,
              isRootDelegation,
              force,
            )
        )
      })

    @Help.Summary("List namespace delegation transactions")
    @Help.Description("""List the namespace delegation transaction present in the stores. Namespace delegations
        |are topology transactions that permission a key to issue topology transactions within
        |a certain namespace.

        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have. State store only has "Add".
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.
        filterNamespace: Filter for namespaces starting with the given filter string.
        filterTargetKey: Filter for namespaces delegations for the given target key.
        protocolVersion: Export the topology transactions in the optional protocol version.
        """)
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterNamespace: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListNamespaceDelegationResult] = {
      val delegations = consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListNamespaceDelegation(
            BaseQuery(
              filterStore,
              useStateStore,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterNamespace,
          )
        )
      }

      // TODO(i9419): Move authorization key filtering to the server side
      filterTargetKey
        .map { targetKey =>
          delegations.filter(_.item.target.fingerprint == targetKey)
        }
        .getOrElse(delegations)
    }
  }

  @Help.Summary("Manage identifier delegations")
  @Help.Group("Identifier delegations")
  object identifier_delegations extends Helpful {

    @Help.Summary("Change identifier delegation")
    @Help.Description("""Delegates the authority of a certain identifier to a certain key. This corresponds to a normal
       |certificate which binds identifier to a key. The keys are referred to using their fingerprints.
       |They need to be either locally generated or have been previously imported.
    ops: Either Add or Remove the delegation.
    signedBy: Refers to the optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
    authorizedKey: Fingerprint of the key to be authorized.
    synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
       """)
    def authorize(
        ops: TopologyChangeOp,
        identifier: UniqueIdentifier,
        authorizedKey: Fingerprint,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): ByteString =
      synchronisation.run(synchronize)(consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AuthorizeIdentifierDelegation(ops, signedBy, identifier, authorizedKey)
        )
      })

    @Help.Summary("List identifier delegation transactions")
    @Help.Description("""List the identifier delegation transaction present in the stores. Identifier delegations
        |are topology transactions that permission a key to issue topology transactions for a certain
        |unique identifier.

        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have. State store only has "Add".
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.
        filterUid: Filter for unique identifiers starting with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |""")
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterUid: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListIdentifierDelegationResult] = {
      val delegations = consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListIdentifierDelegation(
            BaseQuery(
              filterStore,
              useStateStore,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterUid,
          )
        )
      }

      // TODO(i9419): Move authorization key filtering to the server side
      filterTargetKey
        .map { targetKey =>
          delegations.filter(_.item.target.fingerprint == targetKey)
        }
        .getOrElse(delegations)
    }

  }

  @Help.Summary("Manage owner to key mappings")
  @Help.Group("Owner to key mappings")
  object owner_to_key_mappings
      extends OwnerToKeyMappingsGroup(consoleEnvironment.commandTimeouts)
      with Helpful {

    @Help.Summary("Change an owner to key mapping")
    @Help.Description("""Change a owner to key mapping. A key owner is anyone in the system that needs a key-pair known
    |to all members (participants, mediator, sequencer, topology manager) of a domain.
  ops: Either Add or Remove the key mapping update.
  signedBy: Optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
  ownerType: Role of the following owner (Participant, Sequencer, Mediator, DomainTopologyManager)
  owner: Unique identifier of the owner.
  key: Fingerprint of key
  purposes: The purposes of the owner to key mapping.
  force: removing the last key is dangerous and must therefore be manually forced
  synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
  """)
    def authorize(
        ops: TopologyChangeOp,
        keyOwner: KeyOwner,
        key: Fingerprint,
        purpose: KeyPurpose,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        force: Boolean = false,
    ): ByteString =
      synchronisation.run(synchronize)(consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AuthorizeOwnerToKeyMapping(ops, signedBy, keyOwner, key, purpose, force)
        )
      })

    @Help.Summary("List owner to key mapping transactions")
    @Help.Description("""List the owner to key mapping transactions present in the stores. Owner to key mappings
      |are topology transactions defining that a certain key is used by a certain key owner.
      |Key owners are participants, sequencers, mediators and domains.

      filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
      useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
      timeQuery: The time query allows to customize the query by time. The following options are supported:
                 TimeQuery.HeadState (default): The most recent known state.
                 TimeQuery.Snapshot(ts): The state at a certain point in time.
                 TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
      operation: Optionally, what type of operation the transaction should have. State store only has "Add".
      filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.

      filterKeyOwnerType: Filter for a particular type of key owner (KeyOwnerCode).
      filterKeyOwnerUid: Filter for key owners unique identifier starting with the given filter string.
      filterKeyPurpose: Filter for keys with a particular purpose (Encryption or Signing)
      protocolVersion: Export the topology transactions in the optional protocol version.
      |""")
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterKeyOwnerType: Option[KeyOwnerCode] = None,
        filterKeyOwnerUid: String = "",
        filterKeyPurpose: Option[KeyPurpose] = None,
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListOwnerToKeyMappingResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListOwnerToKeyMapping(
            BaseQuery(
              filterStore,
              useStateStore,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterKeyOwnerType,
            filterKeyOwnerUid,
            filterKeyPurpose,
          )
        )
      }

    @Help.Summary("Rotate the key for an owner to key mapping")
    @Help.Description(
      """Rotates the key for an existing owner to key mapping by issuing a new owner to key mapping with the new key
      |and removing the previous owner to key mapping with the previous key.

      nodeInstance: The node instance that is used to verify that both current and new key pertain to this node.
      |This avoids conflicts when there are different nodes with the same uuid (i.e., multiple sequencers).
      owner: The owner of the owner to key mapping
      currentKey: The current public key that will be rotated
      newKey: The new public key that has been generated
      |"""
    )
    def rotate_key(
        nodeInstance: InstanceReferenceCommon,
        owner: KeyOwner,
        currentKey: PublicKey,
        newKey: PublicKey,
    ): Unit = {

      val keysInStore = nodeInstance.keys.secret.list().map(_.publicKey)
      require(
        keysInStore.contains(currentKey),
        "The current key must exist and pertain to this node",
      )
      require(keysInStore.contains(newKey), "The new key must exist and pertain to this node")
      require(currentKey.purpose == newKey.purpose, "The rotated keys must have the same purpose")

      // Authorize the new key
      // The owner will now have two keys, but by convention the first one added is always
      // used by everybody.
      authorize(
        TopologyChangeOp.Add,
        owner,
        newKey.fingerprint,
        newKey.purpose,
      ).discard

      // Remove the old key by sending the matching `Remove` transaction
      authorize(
        TopologyChangeOp.Remove,
        owner,
        currentKey.fingerprint,
        currentKey.purpose,
      ).discard
    }

    def rotate_key(
        owner: KeyOwner,
        currentKey: PublicKey,
        newKey: PublicKey,
    ): Unit =
      throw new IllegalArgumentException(
        s"For this node use `rotate_key` where you specify the node instance"
      )
  }

  @Help.Summary("Manage party to participant mappings")
  @Help.Group("Party to participant mappings")
  object party_to_participant_mappings extends Helpful {

    @Help.Summary("Change party to participant mapping", FeatureFlag.Preview)
    @Help.Description("""Change the association of a party to a participant. If both identifiers are in the same namespace, then the
        |request-side is Both. If they differ, then we need to say whether the request comes from the
        |party (RequestSide.From) or from the participant (RequestSide.To). And, we need the matching request
        |of the other side.
        |Please note that this is a preview feature due to the fact that inhomogeneous topologies can not yet be properly
        |represented on the Ledger API.
      ops: Either Add or Remove the mapping
      signedBy: Refers to the optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
      party: The unique identifier of the party we want to map to a participant.
      participant: The unique identifier of the participant to which the party is supposed to be mapped.
      side: The request side (RequestSide.From if we the transaction is from the perspective of the party, RequestSide.To from the participant.)
      privilege: The privilege of the given participant which allows us to restrict an association (e.g. Confirmation or Observation).
      replaceExisting: If true (default), replace any existing mapping with the new setting
      synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
      """)
    def authorize(
        ops: TopologyChangeOp,
        party: PartyId,
        participant: ParticipantId,
        side: RequestSide = RequestSide.Both,
        permission: ParticipantPermission = ParticipantPermission.Submission,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        replaceExisting: Boolean = true,
        force: Boolean = false,
    ): ByteString =
      check(FeatureFlag.Preview)(synchronisation.run(synchronize)(consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.AuthorizePartyToParticipant(
            ops,
            signedBy,
            side,
            party,
            participant,
            permission,
            replaceExisting = replaceExisting,
            force = force,
          )
        )
      }))

    @Help.Summary("List party to participant mapping transactions")
    @Help.Description(
      """List the party to participant mapping transactions present in the stores. Party to participant mappings
        |are topology transactions used to allocate a party to a certain participant. The same party can be allocated
        |on several participants with different privileges.
        |A party to participant mapping has a request-side that identifies whether the mapping is authorized by the
        |party, by the participant or by both. In order to have a party be allocated to a given participant, we therefore
        |need either two transactions (one with RequestSide.From, one with RequestSide.To) or one with RequestSide.Both.

        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have. State store only has "Add".
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.

        filterParty: Filter for parties starting with the given filter string.
        filterParticipant: Filter for participants starting with the given filter string.
        filterRequestSide: Optional filter for a particular request side (Both, From, To).
        protocolVersion: Export the topology transactions in the optional protocol version.
        |"""
    )
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterParty: String = "",
        filterParticipant: String = "",
        filterRequestSide: Option[RequestSide] = None,
        filterPermission: Option[ParticipantPermission] = None,
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyToParticipantResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListPartyToParticipant(
            BaseQuery(
              filterStore,
              useStateStore,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterParty,
            filterParticipant,
            filterRequestSide,
            filterPermission,
          )
        )
      }
  }
  @Help.Summary("Inspect all topology transactions at once")
  @Help.Group("All Transactions")
  object all extends Helpful {
    @Help.Summary("List all transaction")
    @Help.Description(
      """List all topology transactions in a store, independent of the particular type. This method is useful for
      |exporting entire states.

        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have. State store only has "Add".
        filterAuthorizedKey: Filter the topology transactions by the key that has authorized the transactions.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |"""
    )
    def list(
        filterStore: String = AuthorizedStore.filterName,
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterAuthorizedKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): StoredTopologyTransactions[TopologyChangeOp] = {
      val storedTransactions = consoleEnvironment
        .run {
          adminCommand(
            TopologyAdminCommands.Read.ListAll(
              BaseQuery(
                filterStore,
                useStateStore,
                timeQuery,
                operation,
                filterSigningKey = "",
                protocolVersion.map(ProtocolVersion.tryCreate),
              )
            )
          )
        }

      // TODO(i9419): Move authorization key filtering to the server side
      filterAuthorizedKey
        .map { authKey =>
          val filteredResult =
            storedTransactions.result.filter(_.transaction.key.fingerprint == authKey)
          storedTransactions.copy(result = filteredResult)
        }
        .getOrElse(storedTransactions)
    }

    @Help.Summary(
      "Renew all topology transactions that have been authorized with a previous key using a new key"
    )
    @Help.Description(
      """Finds all topology transactions that have been authorized by `filterAuthorizedKey` and renews those topology transactions
      |by authorizing them with the new key `authorizeWith`.

        filterAuthorizedKey: Filter the topology transactions by the key that has authorized the transactions.
        authorizeWith: The key to authorize the renewed topology transactions.
        |"""
    )
    def renew(
        filterAuthorizedKey: Fingerprint,
        authorizeWith: Fingerprint,
    ): Unit = {

      // First we check that the new key has at least the same permissions as the previous key in terms of namespace
      // delegations and identifier delegations.

      // The namespaces and identifiers that the old key can operate on
      val oldKeyNamespaces = namespace_delegations
        .list(filterStore = AuthorizedStore.filterName, filterTargetKey = filterAuthorizedKey.some)
        .map(_.item.namespace)
      val oldKeyIdentifiers = identifier_delegations
        .list(filterStore = AuthorizedStore.filterName, filterTargetKey = filterAuthorizedKey.some)
        .map(_.item.identifier)

      // The namespaces and identifiers that the new key can operate on
      val newKeyNamespaces = namespace_delegations
        .list(filterStore = AuthorizedStore.filterName, filterTargetKey = authorizeWith.some)
        .map(_.item.namespace)
        .toSet
      val newKeyIdentifiers = identifier_delegations
        .list(filterStore = AuthorizedStore.filterName, filterTargetKey = authorizeWith.some)
        .map(_.item.identifier)
        .toSet

      oldKeyNamespaces.foreach { ns =>
        if (!newKeyNamespaces.contains(ns))
          throw new IllegalArgumentException(
            s"The new key is not authorized for namespace $ns"
          )
      }

      oldKeyIdentifiers.foreach { uid =>
        if (!newKeyIdentifiers.contains(uid) && !newKeyNamespaces.contains(uid.namespace))
          throw new IllegalArgumentException(
            s"The new key is not authorized for the identifier $uid nor the namespace of the identifier ${uid.namespace}"
          )
      }

      val existingTxs = list(filterAuthorizedKey = Some(filterAuthorizedKey))

      // TODO(i9419): Move renewal to the server side
      existingTxs.result
        .foreach { storedTx =>
          storedTx.transaction.transaction match {
            case TopologyStateUpdate(
                  TopologyChangeOp.Add,
                  TopologyStateUpdateElement(_id, update),
                ) =>
              update match {
                case NamespaceDelegation(namespace, target, isRootDelegation) =>
                  def renewNamespaceDelegations(op: TopologyChangeOp, key: Fingerprint): Unit =
                    namespace_delegations
                      .authorize(
                        op,
                        namespace.fingerprint,
                        target.fingerprint,
                        isRootDelegation,
                        key.some,
                      )
                      .discard

                  renewNamespaceDelegations(TopologyChangeOp.Add, authorizeWith)
                  renewNamespaceDelegations(TopologyChangeOp.Remove, filterAuthorizedKey)

                case IdentifierDelegation(identifier, target) =>
                  def renewIdentifierDelegation(op: TopologyChangeOp, key: Fingerprint): Unit =
                    identifier_delegations
                      .authorize(
                        op,
                        identifier,
                        target.fingerprint,
                        key.some,
                      )
                      .discard

                  renewIdentifierDelegation(TopologyChangeOp.Add, authorizeWith)
                  renewIdentifierDelegation(TopologyChangeOp.Remove, filterAuthorizedKey)

                case OwnerToKeyMapping(owner, key) =>
                  def renewOwnerToKeyMapping(op: TopologyChangeOp, nsKey: Fingerprint): Unit =
                    owner_to_key_mappings
                      .authorize(
                        op,
                        owner,
                        key.fingerprint,
                        key.purpose,
                        nsKey.some,
                      )
                      .discard

                  renewOwnerToKeyMapping(TopologyChangeOp.Add, authorizeWith)
                  renewOwnerToKeyMapping(TopologyChangeOp.Remove, filterAuthorizedKey)

                case signedClaim: SignedLegalIdentityClaim =>
                  ()

                case ParticipantState(side, domain, participant, permission, trustLevel) =>
                  def renewParticipantState(op: TopologyChangeOp, key: Fingerprint): Unit =
                    participant_domain_states
                      .authorize(
                        op,
                        domain,
                        participant,
                        side,
                        permission,
                        trustLevel,
                        key.some,
                      )
                      .discard

                  renewParticipantState(TopologyChangeOp.Add, authorizeWith)
                  renewParticipantState(TopologyChangeOp.Remove, filterAuthorizedKey)

                case MediatorDomainState(side, domain, mediator) =>
                  def renewMediatorState(op: TopologyChangeOp, key: Fingerprint): Unit =
                    mediator_domain_states
                      .authorize(
                        op,
                        domain,
                        mediator,
                        side,
                        key.some,
                      )
                      .discard

                  renewMediatorState(TopologyChangeOp.Add, authorizeWith)
                  renewMediatorState(TopologyChangeOp.Remove, filterAuthorizedKey)

                case PartyToParticipant(side, party, participant, permission) =>
                  def renewPartyToParticipant(op: TopologyChangeOp, key: Fingerprint): Unit =
                    party_to_participant_mappings
                      .authorize(
                        op,
                        party,
                        participant,
                        side,
                        permission,
                        key.some,
                      )
                      .discard

                  renewPartyToParticipant(TopologyChangeOp.Add, authorizeWith)
                  renewPartyToParticipant(TopologyChangeOp.Remove, filterAuthorizedKey)

                case VettedPackages(participant, packageIds) =>
                  def renewVettedPackages(op: TopologyChangeOp, key: Fingerprint): Unit =
                    vetted_packages
                      .authorize(
                        op,
                        participant,
                        packageIds,
                        key.some,
                      )
                      .discard

                  renewVettedPackages(TopologyChangeOp.Add, authorizeWith)
                  renewVettedPackages(TopologyChangeOp.Remove, filterAuthorizedKey)
              }

            case TopologyStateUpdate(TopologyChangeOp.Remove, _element) =>
              // We don't have to renew removed topology transactions
              ()

            case DomainGovernanceTransaction(element) =>
              element.mapping match {
                case DomainParametersChange(domainId, domainParameters) =>
                  domain_parameters_changes
                    .authorizeInternal(
                      domainId,
                      domainParameters,
                      authorizeWith.some,
                    )
                    .discard[ByteString]
              }
          }

        }

    }
  }

  @Help.Summary("Inspect participant domain states")
  @Help.Group("Participant Domain States")
  object participant_domain_states extends Helpful {

    @Help.Summary("List participant domain states")
    @Help.Description("""List the participant domain transactions present in the stores. Participant domain states
        |are topology transactions used to permission a participant on a given domain.
        |A participant domain state has a request-side that identifies whether the mapping is authorized by the
        |participant (From), by the domain (To) or by both (Both).
        |In order to use a participant on a domain, both have to authorize such a mapping. This means that by
        |authorizing such a topology transaction, a participant acknowledges its presence on a domain, whereas
        |a domain permissions the participant on that domain.
        |
        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have. State store only has "Add".
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.

        filterDomain: Filter for domains starting with the given filter string.
        filterParticipant: Filter for participants starting with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |""")
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterDomain: String = "",
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListParticipantDomainStateResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListParticipantDomainState(
            BaseQuery(
              filterStore,
              useStateStore,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterDomain,
            filterParticipant,
          )
        )
      }

    @Help.Summary("Change participant domain states")
    @Help.Description("""Change the association of a participant to a domain.
        |In order to activate a participant on a domain, we need both authorisation: the participant authorising
        |its uid to be present on a particular domain and the domain to authorise the presence of a participant
        |on said domain.
        |If both identifiers are in the same namespace, then the request-side can be Both. If they differ, then
        |we need to say whether the request comes from the domain (RequestSide.From) or from the participant
        |(RequestSide.To). And, we need the matching request of the other side.
      ops: Either Add or Remove the mapping
      signedBy: Refers to the optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
      domain: The unique identifier of the domain we want the participant to join.
      participant: The unique identifier of the participant.
      side: The request side (RequestSide.From if we the transaction is from the perspective of the domain, RequestSide.To from the participant.)
      permission: The privilege of the given participant which allows us to restrict an association (e.g. Confirmation or Observation). Will use the lower of if different between To/From.
      trustLevel: The trust level of the participant on the given domain. Will use the lower of if different between To/From.
      replaceExisting: If true (default), replace any existing mapping with the new setting
      synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
      """)
    def authorize(
        ops: TopologyChangeOp,
        domain: DomainId,
        participant: ParticipantId,
        side: RequestSide,
        permission: ParticipantPermission = ParticipantPermission.Submission,
        trustLevel: TrustLevel = TrustLevel.Ordinary,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        replaceExisting: Boolean = true,
    ): ByteString =
      synchronisation.run(synchronize)(consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.AuthorizeParticipantDomainState(
            ops,
            signedBy,
            side,
            domain,
            participant,
            permission,
            trustLevel,
            replaceExisting = replaceExisting,
          )
        )
      })

    @Help.Summary("Returns true if the given participant is currently active on the given domain")
    @Help.Description(
      """Active means that the participant has been granted at least observation rights on the domain
                     |and that the participant has registered a domain trust certificate"""
    )
    def active(domainId: DomainId, participantId: ParticipantId): Boolean = {
      val (notBlocked, from, to) =
        list(
          filterStore = domainId.filterString,
          filterParticipant = participantId.filterString,
        ).iterator
          .map(x => (x.item.side, x.item.permission))
          .foldLeft((true, false, false)) {
            case ((false, _, _), _) | ((_, _, _), (_, ParticipantPermission.Disabled)) =>
              (false, false, false)
            case (_, (RequestSide.Both, _)) => (true, true, true)
            case ((_, _, to), (RequestSide.From, _)) => (true, true, to)
            case ((_, from, _), (RequestSide.To, _)) => (true, from, true)
          }
      notBlocked && from && to
    }
  }

  @Help.Summary("Inspect mediator domain states")
  @Help.Group("Mediator Domain States")
  object mediator_domain_states extends Helpful {

    @Help.Summary("List mediator domain states")
    @Help.Description("""List the mediator domain transactions present in the stores. Mediator domain states
        |are topology transactions used to permission a mediator on a given domain.
        |A mediator domain state has a request-side that identifies whether the mapping is authorized by the
        |mediator (From), by the domain (To) or by both (Both).
        |In order to use a mediator on a domain, both have to authorize such a mapping. This means that by
        |authorizing such a topology transaction, a mediator acknowledges its presence on a domain, whereas
        |a domain permissions the mediator on that domain.
        |
        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have. State store only has "Add".
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.

        filterDomain: Filter for domains starting with the given filter string.
        filterMediator Filter for mediators starting with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |""")
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterDomain: String = "",
        filterMediator: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListMediatorDomainStateResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListMediatorDomainState(
            BaseQuery(
              filterStore,
              useStateStore,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterDomain,
            filterMediator,
          )
        )
      }

    @Help.Summary("Change mediator domain states")
    @Help.Description("""Change the association of a mediator to a domain.
        |In order to activate a mediator on a domain, we need both authorisation: the mediator authorising
        |its uid to be present on a particular domain and the domain to authorise the presence of a mediator
        |on said domain.
        |If both identifiers are in the same namespace, then the request-side can be Both. If they differ, then
        |we need to say whether the request comes from the domain (RequestSide.From) or from the mediator
        |(RequestSide.To). And, we need the matching request of the other side.
      ops: Either Add or Remove the mapping
      signedBy: Refers to the optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
      domain: The unique identifier of the domain we want the mediator to join.
      mediator: The unique identifier of the mediator.
      side: The request side (RequestSide.From if we the transaction is from the perspective of the domain, RequestSide.To from the mediator.)
      replaceExisting: If true (default), replace any existing mapping with the new setting
      synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
      """)
    def authorize(
        ops: TopologyChangeOp,
        domain: DomainId,
        mediator: MediatorId,
        side: RequestSide,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        replaceExisting: Boolean = true,
    ): ByteString =
      synchronisation.run(synchronize)(consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.AuthorizeMediatorDomainState(
            ops,
            signedBy,
            side,
            domain,
            mediator,
            replaceExisting = replaceExisting,
          )
        )
      })
  }

  @Help.Summary("Manage package vettings")
  @Help.Group("Vetted Packages")
  object vetted_packages extends Helpful {

    @Help.Summary("Change package vettings")
    @Help.Description("""A participant will only process transactions that reference packages that all involved participants have
        |vetted previously. Vetting is done by registering a respective topology transaction with the domain,
        |which can then be used by other participants to verify that a transaction is only using
        |vetted packages.
        |Note that all referenced and dependent packages must exist in the package store.
        |By default, only vetting transactions adding new packages can be issued. Removing package vettings
        |and issuing package vettings for other participants (if their identity is controlled through this
        |participants topology manager) or for packages that do not exist locally can only be run using
        |the force = true flag. However, these operations are dangerous and can lead to the situation of a
        |participant being unable to process transactions.
      ops: Either Add or Remove the vetting.
      participant: The unique identifier of the participant that is vetting the package.
      packageIds: The lf-package ids to be vetted.
      signedBy: Refers to the fingerprint of the authorizing key which in turn must be authorized by a valid, locally existing certificate.
      |  If none is given, a key is automatically determined.
      synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
      force: Flag to enable dangerous operations (default false). Great power requires great care.
      """)
    def authorize(
        ops: TopologyChangeOp,
        participant: ParticipantId,
        packageIds: Seq[PackageId],
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        force: Boolean = false,
    ): ByteString =
      synchronisation.run(synchronize)(consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AuthorizeVettedPackages(ops, signedBy, participant, packageIds, force)
        )
      })

    @Help.Summary("List package vetting transactions")
    @Help.Description(
      """List the package vetting transactions present in the stores. Participants must vet Daml packages and submitters
        |must ensure that the receiving participants have vetted the package prior to submitting a transaction (done
        |automatically during submission and validation). Vetting is done by authorizing such topology transactions
        |and registering with a domain.
        |
        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have. State store only has "Add".
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.
        filterParticipant: Filter for participants starting with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |"""
    )
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListVettedPackagesResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListVettedPackages(
            BaseQuery(
              filterStore,
              useStateStore,
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

  @Help.Summary("Manage domain parameters changes", FeatureFlag.Preview)
  @Help.Group("Domain Parameters Changes")
  object domain_parameters_changes extends Helpful {
    @Help.Summary("Change domain parameters")
    @Help.Description("""Authorize a transaction to change parameters of the domain.
      |domainId: Id of the domain affected by the change.
      |newParameters: New value of the domain parameters.
      |protocolVersion: The protocol version of the domain
      |signedBy: Refers to the fingerprint of the authorizing key which in turn must be authorized by a valid, locally existing certificate.
      |          If none is given, a key is automatically determined.
      |synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
      |force: Enable potentially dangerous changes. Required to increase ``ledgerTimeRecordTimeTolerance``.
      |
      |Use ``myDomain.service.set_ledger_time_record_time_tolerance`` to securely increase ``ledgerTimeRecordTimeTolerance``.
      """)
    def authorize(
        domainId: DomainId,
        newParameters: DynamicDomainParameters,
        protocolVersion: ProtocolVersion,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        force: Boolean = false,
    ): ByteString = {
      synchronisation.run(synchronize)(
        consoleEnvironment.run {
          adminCommand(
            TopologyAdminCommands.Write
              .AuthorizeDomainParametersChange(
                signedBy,
                domainId,
                newParameters,
                protocolVersion,
                force,
              )
          )
        }
      )
    }

    // This method accepts parameters in the internal format; used by [[all.renew]] above
    private[TopologyAdministrationGroup] def authorizeInternal(
        domainId: DomainId,
        newParameters: DynamicDomainParametersInternal,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): ByteString = synchronisation.run(synchronize)(
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AuthorizeDomainParametersChangeInternal(
              signedBy,
              domainId,
              newParameters,
              force = false,
            )
        )
      }
    )

    @Help.Summary("Get the latest domain parameters change")
    @Help.Description("""Get the latest domain parameters change for the domain.
      domainId: Id of the domain.
      """)
    def get_latest(domainId: DomainId): DynamicDomainParameters =
      list(filterStore = domainId.filterString)
        .sortBy(_.context.validFrom)(implicitly[Ordering[java.time.Instant]].reverse)
        .headOption
        .map(_.item)
        .getOrElse(
          throw new IllegalStateException("No dynamic domain parameters found in the domain")
        )

    @Help.Summary("List domain parameters changes transactions")
    @Help.Description(
      """List the domain parameters change transactions present in the stores for the specific domain.
        filterStore: Filter for topology stores starting with the given filter string (Authorized, <domain-id>, Requested)
        useStateStore: If true (default), only properly authorized transactions that are part of the state will be selected.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |"""
    )
    def list(
        filterStore: String = "",
        useStateStore: Boolean = true,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListDomainParametersChangeResult] = {
      val baseQuery = BaseQuery(
        filterStore = filterStore,
        useStateStore = useStateStore,
        timeQuery = timeQuery,
        ops = None,
        filterSigningKey = filterSigningKey,
        protocolVersion = protocolVersion.map(ProtocolVersion.tryCreate),
      )

      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListDomainParametersChanges(baseQuery)
        )
      }
    }
  }

}
