// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
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
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
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
    with NamedLogging
    with NoTracing {

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
  ): EitherT[Future, PackageId, Set[PackageId]] = {

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
          val mapping = storedTx.transaction.transaction.mapping
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
      timeQuery = TimeQueryX.Range(None, Some(timestamp)),
      recentTimestampO = None,
      op = Some(TopologyChangeOpX.Replace),
      typ = Some(TopologyMappingX.Code.DomainParametersStateX),
      idFilter = "",
      namespaceOnly = false,
    )
    .map {
      _.collectOfMapping[DomainParametersStateX].result
        .map { storedTx =>
          val dps = storedTx.transaction.transaction.mapping
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
  ): Future[PartyInfo] =
    loadBatchActiveParticipantsOf(Seq(party), participantStates).map(
      _.getOrElse(party, PartyInfo.EmptyPartyInfo)
    )

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[Map[PartyId, PartyInfo]] = {

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
        .groupBy(_.transaction.transaction.mapping.uniqueKey)
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
              participantId -> partyPermission.toNonX
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
                      participantAttributes.trustLevel,
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
  override def mediatorGroups(): Future[Seq[MediatorGroup]] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.MediatorDomainStateX),
    filterUid = None,
    filterNamespace = None,
  ).map(
    _.collectOfMapping[MediatorDomainStateX].result
      .groupBy(_.transaction.transaction.mapping.group)
      .map { case (groupId, seq) =>
        val mds = collectLatestMapping(
          TopologyMappingX.Code.MediatorDomainStateX,
          seq.sortBy(_.validFrom),
        )
          .getOrElse(throw new IllegalStateException("Group-by would not have produced empty seq"))
        MediatorGroup(groupId, mds.active, mds.observers, mds.threshold)
      }
      .toSeq
      .sortBy(_.index)
  )

  override def sequencerGroup(): Future[Option[SequencerGroup]] = findTransactions(
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
  ): Future[Map[Member, Option[MemberTrafficControlState]]] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.TrafficControlStateX),
    filterUid = Some(members.map(_.uid)),
    filterNamespace = None,
  ).map { txs =>
    val membersWithState = txs
      .collectOfMapping[TrafficControlStateX]
      .result
      .groupBy(_.transaction.transaction.mapping.member)
      .flatMap { case (member, mappings) =>
        collectLatestMapping(
          TopologyMappingX.Code.TrafficControlStateX,
          mappings.sortBy(_.validFrom),
        ).map(mapping =>
          Some(MemberTrafficControlState(totalExtraTrafficLimit = mapping.totalExtraTrafficLimit))
        ).map(member -> _)
      }

    val membersWithoutState = members.toSet.diff(membersWithState.keySet).map(_ -> None).toMap

    membersWithState ++ membersWithoutState
  }

  /** Returns a list of all known parties on this domain */
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Set[PartyId]] =
    store.inspectKnownParties(timestamp, filterParty, filterParticipant, limit)

  /** Returns authority-of delegations for consortium parties or self/1 for non consortium parties */
  override def authorityOf(parties: Set[LfPartyId]): Future[AuthorityOfResponse] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.AuthorityOfX),
    filterUid = None,
    filterNamespace = None,
  ).map { transactions =>
    val consortiumDelegations =
      transactions
        .collectOfMapping[AuthorityOfX]
        .result
        .groupBy(_.transaction.transaction.mapping.partyId)
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
  ): Future[Map[Member, KeyCollection]] = {
    store
      .inspect(
        proposals = false,
        timeQuery = TimeQueryX.Snapshot(timestamp),
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
          .groupBy(_.transaction.transaction.mapping.member)
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
                  keys.addTo(key)
                })
          }
      )
  }

  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] =
    loadParticipantStates(Seq(participantId)).map(_.get(participantId))

  private def loadParticipantStatesHelper(
      participantsFilter: Option[Seq[ParticipantId]] // None means fetch all participants
  ): Future[Map[ParticipantId, ParticipantDomainPermissionX]] = for {
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
    // 1. Participant needs to have requested access to domain by issuing a domain trust certificate
    participantsWithCertificates <- findTransactions(
      asOfInclusive = false,
      types = Seq(
        TopologyMappingX.Code.DomainTrustCertificateX
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.collectOfMapping[DomainTrustCertificateX].result
        .groupBy(_.transaction.transaction.mapping.participantId)
        .collect {
          case (pid, seq) if participantsFilter.forall(_.contains(pid)) =>
            // invoke collectLatestMapping only to warn in case a participantId's domain trust certificate is not unique
            collectLatestMapping(
              TopologyMappingX.Code.DomainTrustCertificateX,
              seq.sortBy(_.validFrom),
            ).discard
            pid
        }
        .toSeq
    )
    // 2. Participant needs to have keys registered on the domain
    participantsWithCertAndKeys <- findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMappingX.Code.OwnerToKeyMappingX),
      filterUid = Some(participantsWithCertificates.map(_.uid)),
      filterNamespace = None,
    ).map(
      _.collectOfMapping[OwnerToKeyMappingX].result
        .groupBy(_.transaction.transaction.mapping.member)
        .collect {
          case (pid: ParticipantId, seq)
              if collectLatestMapping(
                TopologyMappingX.Code.OwnerToKeyMappingX,
                seq.sortBy(_.validFrom),
              ).nonEmpty =>
            pid
        }
    )
    // Warn about participants with cert but no keys
    _ = (participantsWithCertificates.toSet -- participantsWithCertAndKeys.toSet).foreach { pid =>
      logger.warn(
        s"Participant ${pid} has a domain trust certificate, but no keys on domain ${domainParametersState.domain}"
      )
    }
    // 3. Attempt to look up permissions/trust from participant domain permission
    participantDomainPermissions <- findTransactions(
      asOfInclusive = false,
      types = Seq(
        TopologyMappingX.Code.ParticipantDomainPermissionX
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.collectOfMapping[ParticipantDomainPermissionX].result
        .groupBy(_.transaction.transaction.mapping.participantId)
        .map { case (pid, seq) =>
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
    )
    // 4. Apply default permissions/trust of submission/ordinary if missing participant domain permission and
    // grab rate limits from dynamic domain parameters if not specified
    participantIdDomainPermissionsMap = participantsWithCertAndKeys.map { pid =>
      pid -> participantDomainPermissions
        .getOrElse(
          pid,
          ParticipantDomainPermissionX.default(domainParametersState.domain, pid),
        )
        .setDefaultLimitIfNotSet(domainParametersState.parameters.v2DefaultParticipantLimits)
    }.toMap
  } yield participantIdDomainPermissionsMap

  /** abstract loading function used to load the participant state for the given set of participant-ids */
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    if (participants.isEmpty)
      Future.successful(Map())
    else
      loadParticipantStatesHelper(participants.some).map(_.map { case (pid, pdp) =>
        pid -> pdp.toParticipantAttributes
      })

  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    Future.failed(
      new UnsupportedOperationException(
        s"Participants lookup not supported by StoreBasedDomainTopologyClientX. This is a coding bug."
      )
    )

  /** abstract loading function used to obtain the full key collection for a key owner */
  override def allKeys(owner: Member): Future[KeyCollection] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.OwnerToKeyMappingX),
    filterUid = Some(Seq(owner.uid)),
    filterNamespace = None,
  )
    .map { transactions =>
      val keys = KeyCollection(Seq(), Seq())
      collectLatestMapping[OwnerToKeyMappingX](
        TopologyMappingX.Code.OwnerToKeyMappingX,
        transactions.collectOfMapping[OwnerToKeyMappingX].result,
      ).fold(keys)(_.keys.foldLeft(keys) { case (keys, key) => keys.addTo(key) })
    }

  override def allMembers(): Future[Set[Member]] = {
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
        .map(_.transaction.transaction.mapping)
        .flatMap {
          case dtc: DomainTrustCertificateX => Seq(dtc.participantId)
          case mds: MediatorDomainStateX => mds.active ++ mds.observers
          case sds: SequencerDomainStateX => sds.active ++ sds.observers
          case _ => Seq.empty
        }
        .toSet
    )
  }

  override def isMemberKnown(member: Member): Future[Boolean] = {
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
            .exists(_.transaction.transaction.mapping.allMediatorsInGroup.contains(mediatorId))
        )
      case sequencerId @ SequencerId(_) =>
        findTransactions(
          asOfInclusive = false,
          types = Seq(SequencerDomainStateX.code),
          filterUid = None,
          filterNamespace = None,
        ).map(
          _.collectOfMapping[SequencerDomainStateX].result
            .exists(_.transaction.transaction.mapping.allSequencers.contains(sequencerId))
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
  ): Option[T] = collectLatestTransaction(typ, transactions).map(_.transaction.transaction.mapping)

  private def collectLatestTransaction[T <: TopologyMappingX](
      typ: TopologyMappingX.Code,
      transactions: Seq[StoredTopologyTransactionX[TopologyChangeOpX.Replace, T]],
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
              s"Instance of \"${typ.code}\" with hash \"${tx.transaction.transaction.hash.hash.toHexString}\" with non-monotonically growing valid-from effective time: previous $previous, new: $validFrom"
            )
          }
          validFrom
        }
        .discard
    }
    transactions.lastOption
  }
}
