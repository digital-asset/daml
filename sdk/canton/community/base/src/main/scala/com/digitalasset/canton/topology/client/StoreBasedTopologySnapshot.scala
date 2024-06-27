// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.functorFilter.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{KeyPurpose, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicDomainParametersWithValidity,
  DynamicSequencingParametersWithValidity,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.{
  AuthorityOfDelegation,
  AuthorityOfResponse,
  PartyInfo,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/** Topology snapshot loader
  *
  * @param timestamp the asOf timestamp to use
  * @param store the db store to use
  * @param packageDependencyResolver provides a way determine the direct and indirect package dependencies
  */
class StoreBasedTopologySnapshot(
    val timestamp: CantonTimestamp,
    store: TopologyStore[TopologyStoreId],
    packageDependencyResolver: PackageDependencyResolverUS,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader
    with NamedLogging {

  private def findTransactions(
      asOfInclusive: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping]] =
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
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] = {

    val vettedF = FutureUnlessShutdown.outcomeF(
      findTransactions(
        asOfInclusive = false,
        types = Seq(TopologyMapping.Code.VettedPackages),
        filterUid = Some(Seq(participant.uid)),
        filterNamespace = None,
      ).map { transactions =>
        collectLatestMapping(
          TopologyMapping.Code.VettedPackages,
          transactions.collectOfMapping[VettedPackages].result,
        ).toList.flatMap(_.packageIds).toSet
      }
    )

    val requiredPackagesF = store.storeId match {
      case _: TopologyStoreId.DomainStore =>
        FutureUnlessShutdown.outcomeF(
          findTransactions(
            asOfInclusive = false,
            types = Seq(TopologyMapping.Code.DomainParametersState),
            filterUid = None,
            filterNamespace = None,
          ).map { transactions =>
            collectLatestMapping(
              TopologyMapping.Code.DomainParametersState,
              transactions.collectOfMapping[DomainParametersState].result,
            ).getOrElse(throw new IllegalStateException("Unable to locate domain parameters state"))
              .discard
            // TODO(#14054) Once the non-proto DynamicDomainParameters is available, use it
            //   _.parameters.requiredPackages
            Set.empty[PackageId]
          }
        )

      case TopologyStoreId.AuthorizedStore =>
        FutureUnlessShutdown.pure(Set.empty)
    }

    lazy val dependenciesET = packageDependencyResolver.packageDependencies(packageId).value

    EitherT(for {
      vetted <- vettedF
      requiredPackages <- requiredPackagesF
      // check that the main package is vetted
      res <-
        if (!vetted.contains(packageId))
          FutureUnlessShutdown.pure(Right(Set(packageId))) // main package is not vetted
        else {
          // check which of the dependencies aren't vetted
          dependenciesET.map(_.map(_ ++ requiredPackages -- vetted))
        }
    } yield res)

  }

  override def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMapping.Code.DomainParametersState),
      filterUid = None,
      filterNamespace = None,
    ).map { transactions =>
      for {
        storedTx <- collectLatestTransaction(
          TopologyMapping.Code.DomainParametersState,
          transactions
            .collectOfMapping[DomainParametersState]
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

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicSequencingParametersWithValidity]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMapping.Code.SequencingDynamicParametersState),
      filterUid = None,
      filterNamespace = None,
    ).map { transactions =>
      for {
        storedTx <- collectLatestTransaction(
          TopologyMapping.Code.SequencingDynamicParametersState,
          transactions
            .collectOfMapping[DynamicSequencingParametersState]
            .result,
        ).toRight(s"Unable to fetch sequencing parameters at $timestamp")

        mapping = storedTx.mapping
      } yield DynamicSequencingParametersWithValidity(
        mapping.parameters,
        storedTx.validFrom.value,
        storedTx.validUntil.map(_.value),
        mapping.domain,
      )
    }

  /** List all the dynamic domain parameters (past and current) */
  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]] = store
    .inspect(
      proposals = false,
      timeQuery = TimeQuery.Range(None, Some(timestamp)),
      recentTimestampO = None,
      op = Some(TopologyChangeOp.Replace),
      types = Seq(TopologyMapping.Code.DomainParametersState),
      idFilter = None,
      namespaceFilter = None,
    )
    .map {
      _.collectOfMapping[DomainParametersState].result
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

    def collectLatestByType[M <: TopologyMapping: ClassTag](
        storedTransactions: StoredTopologyTransactions[
          TopologyChangeOp.Replace,
          TopologyMapping,
        ],
        code: TopologyMapping.Code,
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
              "Group-by would not have produced empty PartyToParticipant seq"
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
          TopologyMapping.Code.PartyToParticipant,
          TopologyMapping.Code.DomainTrustCertificate,
        ),
        filterUid = Some(parties.map(_.uid)),
        filterNamespace = None,
      ).map { storedTransactions =>
        // find normal party declarations
        val partyToParticipantMappings = collectLatestByType[PartyToParticipant](
          storedTransactions,
          TopologyMapping.Code.PartyToParticipant,
        ).map { ptp =>
          ptp.partyId -> (ptp.groupAddressing, ptp.threshold, ptp.participants.map {
            case HostingParticipant(participantId, partyPermission) =>
              participantId -> partyPermission
          }.toMap)
        }.toMap

        // admin parties are implicitly defined by the fact that a participant is available on a domain.
        // admin parties have the same UID as their participant
        val domainTrustCerts = collectLatestByType[DomainTrustCertificate](
          storedTransactions,
          TopologyMapping.Code.DomainTrustCertificate,
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
      types = Seq(TopologyMapping.Code.MediatorDomainState),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.collectOfMapping[MediatorDomainState].result
        .groupBy(_.mapping.group)
        .map { case (groupId, seq) =>
          val mds = collectLatestMapping(
            TopologyMapping.Code.MediatorDomainState,
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
    types = Seq(TopologyMapping.Code.SequencerDomainState),
    filterUid = None,
    filterNamespace = None,
  ).map { transactions =>
    collectLatestMapping(
      TopologyMapping.Code.SequencerDomainState,
      transactions.collectOfMapping[SequencerDomainState].result,
    ).map { (sds: SequencerDomainState) =>
      SequencerGroup(sds.active, sds.observers, sds.threshold)
    }
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
    types = Seq(TopologyMapping.Code.AuthorityOf),
    filterUid = None,
    filterNamespace = None,
  ).map { transactions =>
    val consortiumDelegations =
      transactions
        .collectOfMapping[AuthorityOf]
        .result
        .groupBy(_.mapping.partyId)
        .collect {
          case (partyId, seq) if parties.contains(partyId.toLf) =>
            val authorityOf = collectLatestMapping(
              TopologyMapping.Code.AuthorityOf,
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
    val (idFilter, namespaceFilter) = UniqueIdentifier.splitFilter(filterOwner)
    store
      .inspect(
        proposals = false,
        timeQuery = TimeQuery.Snapshot(timestamp),
        recentTimestampO = None,
        op = Some(TopologyChangeOp.Replace),
        types = Seq(TopologyMapping.Code.OwnerToKeyMapping),
        idFilter = Some(idFilter),
        namespaceFilter = Some(namespaceFilter),
      )
      .map(
        _.collectOfMapping[OwnerToKeyMapping]
          .collectOfType[TopologyChangeOp.Replace]
          .result
          .groupBy(_.mapping.member)
          .collect {
            case (owner, seq)
                if owner.filterString.startsWith(filterOwner)
                  && filterOwnerType.forall(_ == owner.code) =>
              val keys = KeyCollection(Seq(), Seq())
              val okm =
                collectLatestMapping(
                  TopologyMapping.Code.OwnerToKeyMapping,
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
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping]
  )(implicit traceContext: TraceContext): Set[ParticipantId] = storedTxs
    .collectOfMapping[DomainTrustCertificate]
    .result
    .groupBy(_.mapping.participantId)
    .collect { case (pid, seq) =>
      // invoke collectLatestMapping only to warn in case a participantId's domain trust certificate is not unique
      collectLatestMapping(
        TopologyMapping.Code.DomainTrustCertificate,
        seq.sortBy(_.validFrom),
      ).discard
      pid
    }
    .toSet

  private def getParticipantsWithCertAndKeys(
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping],
      participantsWithCertificates: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Set[ParticipantId] = {
    storedTxs
      .collectOfMapping[OwnerToKeyMapping]
      .result
      .groupBy(_.mapping.member)
      .collect {
        case (pid: ParticipantId, seq)
            if participantsWithCertificates(pid) && collectLatestMapping(
              TopologyMapping.Code.OwnerToKeyMapping,
              seq.sortBy(_.validFrom),
            ).exists(otk =>
              keysRequiredForParticipants.diff(otk.keys.forgetNE.map(_.purpose).toSet).isEmpty
            ) =>
          pid
      }
      .toSet
  }

  private def getParticipantDomainPermissions(
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping],
      participantsWithCertAndKeys: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Map[ParticipantId, ParticipantDomainPermission] = {
    storedTxs
      .collectOfMapping[ParticipantDomainPermission]
      .result
      .groupBy(_.mapping.participantId)
      .collect {
        case (pid, seq) if participantsWithCertAndKeys(pid) =>
          val mapping =
            collectLatestMapping(
              TopologyMapping.Code.ParticipantDomainPermission,
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
  ): Future[Map[ParticipantId, ParticipantDomainPermission]] = {
    for {
      // Looks up domain parameters for default rate limits.
      domainParametersState <- findTransactions(
        asOfInclusive = false,
        types = Seq(
          TopologyMapping.Code.DomainParametersState
        ),
        filterUid = None,
        filterNamespace = None,
      ).map(transactions =>
        collectLatestMapping(
          TopologyMapping.Code.DomainParametersState,
          transactions.collectOfMapping[DomainParametersState].result,
        ).getOrElse(throw new IllegalStateException("Unable to locate domain parameters state"))
      )
      storedTxs <- findTransactions(
        asOfInclusive = false,
        types = Seq(
          TopologyMapping.Code.DomainTrustCertificate,
          TopologyMapping.Code.OwnerToKeyMapping,
          TopologyMapping.Code.ParticipantDomainPermission,
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
            ParticipantDomainPermission.default(domainParametersState.domain, pid),
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

  /** abstract loading function used to obtain the full key collection for a key owner */
  override def allKeys(owner: Member)(implicit traceContext: TraceContext): Future[KeyCollection] =
    allKeys(Seq(owner)).map(_.getOrElse(owner, KeyCollection.empty))

  override def allKeys(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): Future[Map[Member, KeyCollection]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMapping.Code.OwnerToKeyMapping),
      filterUid = Some(members.map(_.uid)),
      filterNamespace = None,
    ).map { transactions =>
      transactions
        .collectOfMapping[OwnerToKeyMapping]
        .result
        .groupBy(_.mapping.member)
        .map { case (member, otks) =>
          val keys = collectLatestMapping[OwnerToKeyMapping](
            TopologyMapping.Code.OwnerToKeyMapping,
            otks.sortBy(_.validFrom),
          ).toList.flatMap(_.keys.forgetNE)
          member -> KeyCollection.empty.addAll(keys)
        }
    }

  override def allMembers()(implicit traceContext: TraceContext): Future[Set[Member]] = {
    findTransactions(
      asOfInclusive = false,
      types = Seq(
        DomainTrustCertificate.code,
        MediatorDomainState.code,
        SequencerDomainState.code,
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.result.view
        .map(_.mapping)
        .flatMap {
          case dtc: DomainTrustCertificate => Seq(dtc.participantId)
          case mds: MediatorDomainState => mds.active ++ mds.observers
          case sds: SequencerDomainState => sds.active ++ sds.observers
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
          types = Seq(DomainTrustCertificate.code),
          filterUid = Some(Seq(pid)),
          filterNamespace = None,
        ).map(_.result.nonEmpty)
      case mediatorId @ MediatorId(_) =>
        findTransactions(
          asOfInclusive = false,
          types = Seq(MediatorDomainState.code),
          filterUid = None,
          filterNamespace = None,
        ).map(
          _.collectOfMapping[MediatorDomainState].result
            .exists(_.mapping.allMediatorsInGroup.contains(mediatorId))
        )
      case sequencerId @ SequencerId(_) =>
        findTransactions(
          asOfInclusive = false,
          types = Seq(SequencerDomainState.code),
          filterUid = None,
          filterNamespace = None,
        ).map(
          _.collectOfMapping[SequencerDomainState].result
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

  override def memberFirstKnownAt(
      member: Member
  )(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] = {
    member match {
      case participantId: ParticipantId =>
        store.findFirstTrustCertificateForParticipant(participantId).map(_.map(_.validFrom.value))
      case mediatorId: MediatorId =>
        store.findFirstMediatorStateForMediator(mediatorId).map(_.map(_.validFrom.value))
      case sequencerId: SequencerId =>
        store.findFirstSequencerStateForSequencer(sequencerId).map(_.map(_.validFrom.value))
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"Checking whether member is known for an unexpected member type: $member"
          )
        )
    }
  }

  private def collectLatestMapping[T <: TopologyMapping](
      typ: TopologyMapping.Code,
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp.Replace, T]],
  )(implicit traceContext: TraceContext): Option[T] =
    collectLatestTransaction(typ, transactions).map(_.mapping)

  private def collectLatestTransaction[T <: TopologyMapping](
      typ: TopologyMapping.Code,
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp.Replace, T]],
  )(implicit
      traceContext: TraceContext
  ): Option[StoredTopologyTransaction[TopologyChangeOp.Replace, T]] = {
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

  override def signingKeysUS(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[SigningPublicKey]] = FutureUnlessShutdown.outcomeF(signingKeys(owner))
}
