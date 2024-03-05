// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.functorFilter.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.KeyPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParametersWithValidity
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.{
  AuthorityOfDelegation,
  AuthorityOfResponse,
  PartyInfo,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOpX.Replace
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/** The domain topology client that reads data from a topology store
  *
  * @param domainId The domain-id corresponding to this store
  * @param store The store
  */
class StoreBasedDomainTopologyClientX(
    val clock: Clock,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    store: TopologyStoreX[TopologyStoreId],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends BaseDomainTopologyClientX
    with NamedLogging {

  override def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): StoreBasedTopologySnapshotX = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, topology known until=$topologyKnownUntilTimestamp",
    )
    new StoreBasedTopologySnapshotX(
      timestamp,
      store,
      packageDependencies,
      loggerFactory,
    )
  }

}

/** Topology snapshot loader
  *
  * @param timestamp the asOf timestamp to use
  * @param store the db store to use
  * @param packageDependencies lookup function to determine the direct and indirect package dependencies
  */
class StoreBasedTopologySnapshotX(
    val timestamp: CantonTimestamp,
    store: TopologyStoreX[TopologyStoreId],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader
    with NamedLogging {

  private def findTransactions(
      asOfInclusive: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX.Replace, TopologyMappingX]] =
    store
      .findPositiveTransactions(
        timestamp,
        asOfInclusive,
        isProposal = false,
        types,
        filterUid,
        filterNamespace,
      )

  override private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  )(implicit traceContext: TraceContext): EitherT[Future, PackageId, Set[PackageId]] = {

    val vettedET = EitherT.right[PackageId](
      findTransactions(
        asOfInclusive = false,
        types = Seq(TopologyMappingX.Code.VettedPackagesX),
        filterUid = Some(Seq(participant.uid)),
        filterNamespace = None,
      ).map { transactions =>
        collectLatestMapping(
          TopologyMappingX.Code.VettedPackagesX,
          transactions.collectOfMapping[VettedPackagesX].result,
        ).toList.flatMap(_.packageIds).toSet
      }
    )

    val requiredPackagesET = store.storeId match {
      case _: TopologyStoreId.DomainStore =>
        EitherT.right[PackageId](
          findTransactions(
            asOfInclusive = false,
            types = Seq(TopologyMappingX.Code.DomainParametersStateX),
            filterUid = None,
            filterNamespace = None,
          ).map { transactions =>
            collectLatestMapping(
              TopologyMappingX.Code.DomainParametersStateX,
              transactions.collectOfMapping[DomainParametersStateX].result,
            ).getOrElse(throw new IllegalStateException("Unable to locate domain parameters state"))
              .discard

            // TODO(#14054) Once the non-proto DynamicDomainParametersX is available, use it
            //   _.parameters.requiredPackages
            Seq.empty[PackageId]
          }
        )

      case TopologyStoreId.AuthorizedStore =>
        EitherT.pure[Future, PackageId](Seq.empty)
    }

    lazy val dependenciesET = packageDependencies(packageId)

    for {
      vetted <- vettedET
      requiredPackages <- requiredPackagesET
      // check that the main package is vetted
      res <-
        if (!vetted.contains(packageId))
          EitherT.rightT[Future, PackageId](Set(packageId)) // main package is not vetted
        else {
          // check which of the dependencies aren't vetted
          dependenciesET.map(deps => (deps ++ requiredPackages) -- vetted)
        }
    } yield res

  }

  override def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMappingX.Code.DomainParametersStateX),
      filterUid = None,
      filterNamespace = None,
    ).map { transactions =>
      for {
        storedTx <- collectLatestTransaction(
          TopologyMappingX.Code.DomainParametersStateX,
          transactions
            .collectOfMapping[DomainParametersStateX]
            .result,
        ).toRight(s"Unable to fetch domain parameters at $timestamp")

        domainParameters = {
          val mapping = storedTx.mapping
          DynamicDomainParametersWithValidity(
            mapping.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
            mapping.domain,
          )
        }
      } yield domainParameters
    }

  /** List all the dynamic domain parameters (past and current) */
  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]] = store
    .inspect(
      proposals = false,
      timeQuery = TimeQuery.Range(None, Some(timestamp)),
      recentTimestampO = None,
      op = Some(TopologyChangeOpX.Replace),
      typ = Some(TopologyMappingX.Code.DomainParametersStateX),
      idFilter = "",
      namespaceOnly = false,
    )
    .map {
      _.collectOfMapping[DomainParametersStateX].result
        .map { storedTx =>
          val dps = storedTx.mapping
          DynamicDomainParametersWithValidity(
            dps.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
            dps.domain,
          )
        }
    }

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext): Future[PartyInfo] =
    loadBatchActiveParticipantsOf(Seq(party), participantStates).map(
      _.getOrElse(party, PartyInfo.EmptyPartyInfo)
    )

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext): Future[Map[PartyId, PartyInfo]] = {

    def collectLatestByType[M <: TopologyMappingX: ClassTag](
        storedTransactions: StoredTopologyTransactionsX[
          TopologyChangeOpX.Replace,
          TopologyMappingX,
        ],
        code: TopologyMappingX.Code,
    ): Seq[M] = {
      storedTransactions
        .collectOfMapping[M]
        .result
        .groupBy(_.mapping.uniqueKey)
        .map { case (_, seq) =>
          collectLatestMapping[M](
            code,
            seq.sortBy(_.validFrom),
          ).getOrElse(
            throw new IllegalStateException(
              "Group-by would not have produced empty PartyToParticipantX seq"
            )
          )
        }
        .toSeq
    }

    for {
      // get all party to participant mappings and also participant states for this uid (latter to mix in admin parties)
      partyData <- findTransactions(
        asOfInclusive = false,
        types = Seq(
          TopologyMappingX.Code.PartyToParticipantX,
          TopologyMappingX.Code.DomainTrustCertificateX,
        ),
        filterUid = Some(parties.map(_.uid)),
        filterNamespace = None,
      ).map { storedTransactions =>
        // find normal party declarations
        val partyToParticipantMappings = collectLatestByType[PartyToParticipantX](
          storedTransactions,
          TopologyMappingX.Code.PartyToParticipantX,
        ).map { ptp =>
          ptp.partyId -> (ptp.groupAddressing, ptp.threshold, ptp.participants.map {
            case HostingParticipant(participantId, partyPermission) =>
              participantId -> partyPermission
          }.toMap)
        }.toMap

        // admin parties are implicitly defined by the fact that a participant is available on a domain.
        // admin parties have the same UID as their participant
        val domainTrustCerts = collectLatestByType[DomainTrustCertificateX](
          storedTransactions,
          TopologyMappingX.Code.DomainTrustCertificateX,
        ).map(cert => cert.participantId)

        (partyToParticipantMappings, domainTrustCerts)
      }
      (partyToParticipantMap, adminPartyParticipants) = partyData

      // fetch all admin parties
      participantIds = partyToParticipantMap.values
        .map(_._3)
        .flatMap(_.keys)
        .toSeq ++ adminPartyParticipants

      participantToAttributesMap <- loadParticipantStates(participantIds)

      adminPartiesMap = adminPartyParticipants
        .mapFilter(participantId =>
          participantToAttributesMap
            .get(participantId)
            .map(attrs =>
              // participant admin parties are never consortium parties
              participantId.adminParty -> PartyInfo
                .nonConsortiumPartyInfo(Map(participantId -> attrs))
            )
        )
        .toMap

      // In case the party->participant mapping contains participants missing from map returned
      // by loadParticipantStates, filter out participants with "empty" permissions and transitively
      // parties whose participants have all been filtered out this way.
      // this can only affect participants that have left the domain
      partiesToPartyInfos = {
        val p2pMappings = partyToParticipantMap.toSeq.mapFilter {
          case (partyId, (groupAddressing, threshold, participantToPermissionsMap)) =>
            val participantIdToAttribs = participantToPermissionsMap.toSeq.mapFilter {
              case (participantId, partyPermission) =>
                participantToAttributesMap
                  .get(participantId)
                  .map { participantAttributes =>
                    // Use the lower permission between party and the permission granted to the participant by the domain
                    val reducedPermission = {
                      ParticipantPermission.lowerOf(
                        partyPermission,
                        participantAttributes.permission,
                      )
                    }
                    participantId -> ParticipantAttributes(
                      reducedPermission,
                      None,
                    )
                  }
            }.toMap
            if (participantIdToAttribs.isEmpty) None
            else Some(partyId -> PartyInfo(groupAddressing, threshold, participantIdToAttribs))
        }.toMap
        p2pMappings ++
          adminPartiesMap.collect {
            // TODO(#12390) - Remove this extra caution not to add admin parties if somehow they already exist as p2p
            //  mappings once corresponding validation is in place.
            case x @ (adminPartyId, _) if !p2pMappings.contains(adminPartyId) => x
          }
      }
      // For each party we must return a result to satisfy the expectations of the
      // calling CachingTopologySnapshot's caffeine partyCache per findings in #11598.
      // This includes parties not found in the topology store or parties filtered out
      // above, e.g. parties whose participants have left the domain.
      fullySpecifiedPartyMap = parties.map { party =>
        party -> partiesToPartyInfos.getOrElse(party, PartyInfo.EmptyPartyInfo)
      }.toMap
    } yield fullySpecifiedPartyMap
  }

  /** returns the list of currently known mediator groups */
  override def mediatorGroups()(implicit traceContext: TraceContext): Future[Seq[MediatorGroup]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMappingX.Code.MediatorDomainStateX),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.collectOfMapping[MediatorDomainStateX].result
        .groupBy(_.mapping.group)
        .map { case (groupId, seq) =>
          val mds = collectLatestMapping(
            TopologyMappingX.Code.MediatorDomainStateX,
            seq.sortBy(_.validFrom),
          )
            .getOrElse(
              throw new IllegalStateException("Group-by would not have produced empty seq")
            )
          MediatorGroup(groupId, mds.active, mds.observers, mds.threshold)
        }
        .toSeq
        .sortBy(_.index)
    )

  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): Future[Option[SequencerGroup]] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.SequencerDomainStateX),
    filterUid = None,
    filterNamespace = None,
  ).map { transactions =>
    collectLatestMapping(
      TopologyMappingX.Code.SequencerDomainStateX,
      transactions.collectOfMapping[SequencerDomainStateX].result,
    ).map { (sds: SequencerDomainStateX) =>
      SequencerGroup(sds.active, sds.observers, sds.threshold)
    }
  }

  def trafficControlStatus(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): Future[Map[Member, Option[MemberTrafficControlState]]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMappingX.Code.TrafficControlStateX),
      filterUid = Some(members.map(_.uid)),
      filterNamespace = None,
    ).map { txs =>
      val membersWithState = txs
        .collectOfMapping[TrafficControlStateX]
        .result
        .groupBy(_.mapping.member)
        .flatMap { case (member, mappings) =>
          collectLatestTransaction(
            TopologyMappingX.Code.TrafficControlStateX,
            mappings.sortBy(_.validFrom),
          ).map { tx =>
            val mapping = tx.mapping
            Some(
              MemberTrafficControlState(
                totalExtraTrafficLimit = mapping.totalExtraTrafficLimit,
                tx.serial,
                tx.validFrom.value,
              )
            )
          }.map(member -> _)
        }

      val membersWithoutState = members.toSet.diff(membersWithState.keySet).map(_ -> None).toMap

      membersWithState ++ membersWithoutState
    }

  /** Returns a list of all known parties on this domain */
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] =
    store.inspectKnownParties(timestamp, filterParty, filterParticipant, limit)

  /** Returns authority-of delegations for consortium parties or self/1 for non consortium parties */
  override def authorityOf(
      parties: Set[LfPartyId]
  )(implicit traceContext: TraceContext): Future[AuthorityOfResponse] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.AuthorityOfX),
    filterUid = None,
    filterNamespace = None,
  ).map { transactions =>
    val consortiumDelegations =
      transactions
        .collectOfMapping[AuthorityOfX]
        .result
        .groupBy(_.mapping.partyId)
        .collect {
          case (partyId, seq) if parties.contains(partyId.toLf) =>
            val authorityOf = collectLatestMapping(
              TopologyMappingX.Code.AuthorityOfX,
              seq.sortBy(_.validFrom),
            )
              .getOrElse(
                throw new IllegalStateException("Group-by would not have produced empty seq")
              )
            partyId.toLf -> AuthorityOfDelegation(
              authorityOf.parties.map(_.toLf).toSet,
              authorityOf.threshold,
            )
        }
    // If not all parties are consortium parties, fall back to the behavior of the PartyTopologySnapshotClient super trait
    // to produce a mapping to self with threshold one (without checking whether the party exists on the domain).
    val nonConsortiumPartyDelegations = (parties -- consortiumDelegations.keys)
      .map(partyId => partyId -> PartyTopologySnapshotClient.nonConsortiumPartyDelegation(partyId))
      .toMap
    AuthorityOfResponse(consortiumDelegations ++ nonConsortiumPartyDelegations)
  }

  /** Returns a list of owner's keys (at most limit) */
  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Map[Member, KeyCollection]] = {
    store
      .inspect(
        proposals = false,
        timeQuery = TimeQuery.Snapshot(timestamp),
        recentTimestampO = None,
        op = Some(TopologyChangeOpX.Replace),
        typ = Some(TopologyMappingX.Code.OwnerToKeyMappingX),
        idFilter = filterOwner,
        namespaceOnly = false,
      )
      .map(
        _.collectOfMapping[OwnerToKeyMappingX]
          .collectOfType[TopologyChangeOpX.Replace]
          .result
          .groupBy(_.mapping.member)
          .collect {
            case (owner, seq)
                if owner.filterString.startsWith(filterOwner)
                  && filterOwnerType.forall(_ == owner.code) =>
              val keys = KeyCollection(Seq(), Seq())
              val okm =
                collectLatestMapping(
                  TopologyMappingX.Code.OwnerToKeyMappingX,
                  seq.sortBy(_.validFrom),
                )
              owner -> okm
                .fold(keys)(_.keys.take(limit).foldLeft(keys) { case (keys, key) =>
                  keys.add(key)
                })
          }
      )
  }

  private val keysRequiredForParticipants = Set(KeyPurpose.Signing, KeyPurpose.Encryption)

  private def getParticipantsWithCertificates(
      storedTxs: StoredTopologyTransactionsX[Replace, TopologyMappingX]
  )(implicit traceContext: TraceContext): Set[ParticipantId] = storedTxs
    .collectOfMapping[DomainTrustCertificateX]
    .result
    .groupBy(_.mapping.participantId)
    .collect { case (pid, seq) =>
      // invoke collectLatestMapping only to warn in case a participantId's domain trust certificate is not unique
      collectLatestMapping(
        TopologyMappingX.Code.DomainTrustCertificateX,
        seq.sortBy(_.validFrom),
      ).discard
      pid
    }
    .toSet

  private def getParticipantsWithCertAndKeys(
      storedTxs: StoredTopologyTransactionsX[Replace, TopologyMappingX],
      participantsWithCertificates: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Set[ParticipantId] = {
    storedTxs
      .collectOfMapping[OwnerToKeyMappingX]
      .result
      .groupBy(_.mapping.member)
      .collect {
        case (pid: ParticipantId, seq)
            if participantsWithCertificates(pid) && collectLatestMapping(
              TopologyMappingX.Code.OwnerToKeyMappingX,
              seq.sortBy(_.validFrom),
            ).exists(otk =>
              keysRequiredForParticipants.diff(otk.keys.forgetNE.map(_.purpose).toSet).isEmpty
            ) =>
          pid
      }
      .toSet
  }

  private def getParticipantDomainPermissions(
      storedTxs: StoredTopologyTransactionsX[Replace, TopologyMappingX],
      participantsWithCertAndKeys: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Map[ParticipantId, ParticipantDomainPermissionX] = {
    storedTxs
      .collectOfMapping[ParticipantDomainPermissionX]
      .result
      .groupBy(_.mapping.participantId)
      .collect {
        case (pid, seq) if participantsWithCertAndKeys(pid) =>
          val mapping =
            collectLatestMapping(
              TopologyMappingX.Code.ParticipantDomainPermissionX,
              seq.sortBy(_.validFrom),
            )
              .getOrElse(
                throw new IllegalStateException("Group-by would not have produced empty seq")
              )
          pid -> mapping
      }

  }
  private def loadParticipantStatesHelper(
      participantsFilter: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): Future[Map[ParticipantId, ParticipantDomainPermissionX]] = {
    for {
      // Looks up domain parameters for default rate limits.
      domainParametersState <- findTransactions(
        asOfInclusive = false,
        types = Seq(
          TopologyMappingX.Code.DomainParametersStateX
        ),
        filterUid = None,
        filterNamespace = None,
      ).map(transactions =>
        collectLatestMapping(
          TopologyMappingX.Code.DomainParametersStateX,
          transactions.collectOfMapping[DomainParametersStateX].result,
        ).getOrElse(throw new IllegalStateException("Unable to locate domain parameters state"))
      )
      storedTxs <- findTransactions(
        asOfInclusive = false,
        types = Seq(
          TopologyMappingX.Code.DomainTrustCertificateX,
          TopologyMappingX.Code.OwnerToKeyMappingX,
          TopologyMappingX.Code.ParticipantDomainPermissionX,
        ),
        filterUid = Some(participantsFilter.map(_.uid)),
        filterNamespace = None,
      )

    } yield {
      // 1. Participant needs to have requested access to domain by issuing a domain trust certificate
      val participantsWithCertificates = getParticipantsWithCertificates(storedTxs)
      // 2. Participant needs to have keys registered on the domain
      val participantsWithCertAndKeys =
        getParticipantsWithCertAndKeys(storedTxs, participantsWithCertificates)
      // Warn about participants with cert but no keys
      (participantsWithCertificates -- participantsWithCertAndKeys).foreach { pid =>
        logger.warn(
          s"Participant ${pid} has a domain trust certificate, but no keys on domain ${domainParametersState.domain}"
        )
      }
      // 3. Attempt to look up permissions/trust from participant domain permission
      val participantDomainPermissions =
        getParticipantDomainPermissions(storedTxs, participantsWithCertAndKeys)
      // 4. Apply default permissions/trust of submission/ordinary if missing participant domain permission and
      // grab rate limits from dynamic domain parameters if not specified
      val participantIdDomainPermissionsMap = participantsWithCertAndKeys.map { pid =>
        pid -> participantDomainPermissions
          .getOrElse(
            pid,
            ParticipantDomainPermissionX.default(domainParametersState.domain, pid),
          )
          .setDefaultLimitIfNotSet(domainParametersState.parameters.v2DefaultParticipantLimits)
      }.toMap
      participantIdDomainPermissionsMap
    }
  }

  /** abstract loading function used to load the participant state for the given set of participant-ids */
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit traceContext: TraceContext): Future[Map[ParticipantId, ParticipantAttributes]] =
    if (participants.isEmpty)
      Future.successful(Map())
    else
      loadParticipantStatesHelper(participants).map(_.map { case (pid, pdp) =>
        pid -> pdp.toParticipantAttributes
      })

  override def participants()(implicit
      traceContext: TraceContext
  ): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    Future.failed(
      new UnsupportedOperationException(
        s"Participants lookup not supported by StoreBasedDomainTopologyClientX. This is a coding bug."
      )
    )

  /** abstract loading function used to obtain the full key collection for a key owner */
  override def allKeys(owner: Member)(implicit traceContext: TraceContext): Future[KeyCollection] =
    allKeys(Seq(owner)).map(_.getOrElse(owner, KeyCollection.empty))

  override def allKeys(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): Future[Map[Member, KeyCollection]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMappingX.Code.OwnerToKeyMappingX),
      filterUid = Some(members.map(_.uid)),
      filterNamespace = None,
    ).map { transactions =>
      transactions
        .collectOfMapping[OwnerToKeyMappingX]
        .result
        .groupBy(_.mapping.member)
        .map { case (member, otks) =>
          val keys = collectLatestMapping[OwnerToKeyMappingX](
            TopologyMappingX.Code.OwnerToKeyMappingX,
            otks.sortBy(_.validFrom),
          ).toList.flatMap(_.keys.forgetNE)
          member -> KeyCollection.empty.addAll(keys)
        }
    }

  override def allMembers()(implicit traceContext: TraceContext): Future[Set[Member]] = {
    findTransactions(
      asOfInclusive = false,
      types = Seq(
        DomainTrustCertificateX.code,
        MediatorDomainStateX.code,
        SequencerDomainStateX.code,
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.result.view
        .map(_.mapping)
        .flatMap {
          case dtc: DomainTrustCertificateX => Seq(dtc.participantId)
          case mds: MediatorDomainStateX => mds.active ++ mds.observers
          case sds: SequencerDomainStateX => sds.active ++ sds.observers
          case _ => Seq.empty
        }
        .toSet
    )
  }

  override def isMemberKnown(
      member: Member
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    member match {
      case ParticipantId(pid) =>
        findTransactions(
          asOfInclusive = false,
          types = Seq(DomainTrustCertificateX.code),
          filterUid = Some(Seq(pid)),
          filterNamespace = None,
        ).map(_.result.nonEmpty)
      case mediatorId @ MediatorId(_) =>
        findTransactions(
          asOfInclusive = false,
          types = Seq(MediatorDomainStateX.code),
          filterUid = None,
          filterNamespace = None,
        ).map(
          _.collectOfMapping[MediatorDomainStateX].result
            .exists(_.mapping.allMediatorsInGroup.contains(mediatorId))
        )
      case sequencerId @ SequencerId(_) =>
        findTransactions(
          asOfInclusive = false,
          types = Seq(SequencerDomainStateX.code),
          filterUid = None,
          filterNamespace = None,
        ).map(
          _.collectOfMapping[SequencerDomainStateX].result
            .exists(_.mapping.allSequencers.contains(sequencerId))
        )
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"Checking whether member is known for an unexpected member type: $member"
          )
        )
    }
  }

  private def collectLatestMapping[T <: TopologyMappingX](
      typ: TopologyMappingX.Code,
      transactions: Seq[StoredTopologyTransactionX[TopologyChangeOpX.Replace, T]],
  )(implicit traceContext: TraceContext): Option[T] =
    collectLatestTransaction(typ, transactions).map(_.mapping)

  private def collectLatestTransaction[T <: TopologyMappingX](
      typ: TopologyMappingX.Code,
      transactions: Seq[StoredTopologyTransactionX[TopologyChangeOpX.Replace, T]],
  )(implicit
      traceContext: TraceContext
  ): Option[StoredTopologyTransactionX[TopologyChangeOpX.Replace, T]] = {
    if (transactions.sizeCompare(1) > 0) {
      logger.warn(
        s"Expected unique \"${typ.code}\" at $referenceTime, but found multiple instances"
      )
      transactions
        .foldLeft(CantonTimestamp.Epoch) { case (previous, tx) =>
          val validFrom = tx.validFrom.value
          if (previous >= validFrom) {
            logger.warn(
              s"Instance of \"${typ.code}\" with hash \"${tx.hash.hash.toHexString}\" with non-monotonically growing valid-from effective time: previous $previous, new: $validFrom"
            )
          }
          validFrom
        }
        .discard
    }
    transactions.lastOption
  }

}
