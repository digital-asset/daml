// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.syntax.functorFilter.*
import com.digitalasset.canton.crypto.{KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext
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
      types: Seq[TopologyMapping.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping]] =
    store
      .findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types,
        filterUid,
        filterNamespace,
      )

  override private[client] def loadVettedPackages(
      participant: ParticipantId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[PackageId, VettedPackage]] =
    findTransactions(
      types = Seq(TopologyMapping.Code.VettedPackages),
      filterUid = Some(Seq(participant.uid)),
      filterNamespace = None,
    ).map { transactions =>
      collectLatestMapping(
        TopologyMapping.Code.VettedPackages,
        transactions.collectOfMapping[VettedPackages].result,
      ).toList.flatMap(_.packages.map(vp => (vp.packageId, vp))).toMap
    }

  override private[client] def loadUnvettedPackagesOrDependenciesUsingLoader(
      participant: ParticipantId,
      packageId: PackageId,
      ledgerTime: CantonTimestamp,
      vettedPackagesLoader: VettedPackagesLoader,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[PackageId]] =
    for {
      vetted <- vettedPackagesLoader.loadVettedPackages(participant)
      validAtLedgerTime = (pkg: PackageId) => vetted.get(pkg).exists(_.validAt(ledgerTime))
      // check that the main package is vetted
      res <-
        if (!validAtLedgerTime(packageId))
          // main package is not vetted
          FutureUnlessShutdown.pure(Set(packageId))
        else {
          // check which of the dependencies aren't vetted
          packageDependencyResolver
            .packageDependencies(packageId)
            .map(dependencies => dependencies.filter(dependency => !validAtLedgerTime(dependency)))
            .leftMap(Set(_))
            .merge

        }
    } yield res

  override def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
    findTransactions(
      types = Seq(TopologyMapping.Code.SynchronizerParametersState),
      filterUid = None,
      filterNamespace = None,
    ).map { transactions =>
      for {
        storedTx <- collectLatestTransaction(
          TopologyMapping.Code.SynchronizerParametersState,
          transactions
            .collectOfMapping[SynchronizerParametersState]
            .result,
        ).toRight(s"Unable to fetch synchronizer parameters at $timestamp")

        synchronizerParameters = {
          val mapping = storedTx.mapping
          DynamicSynchronizerParametersWithValidity(
            mapping.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
            mapping.synchronizerId,
          )
        }
      } yield synchronizerParameters
    }

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
    findTransactions(
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
        mapping.synchronizerId,
      )
    }

  /** List all the dynamic synchronizer parameters (past and current) */
  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] = store
    .inspect(
      proposals = false,
      timeQuery = TimeQuery.Range(None, Some(timestamp)),
      asOfExclusiveO = None,
      op = Some(TopologyChangeOp.Replace),
      types = Seq(TopologyMapping.Code.SynchronizerParametersState),
      idFilter = None,
      namespaceFilter = None,
    )
    .map {
      _.collectOfMapping[SynchronizerParametersState].result
        .map { storedTx =>
          val dps = storedTx.mapping
          DynamicSynchronizerParametersWithValidity(
            dps.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
            dps.synchronizerId,
          )
        }
    }

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo] =
    loadBatchActiveParticipantsOf(Seq(party), participantStates).map(
      _.getOrElse(party, PartyInfo.EmptyPartyInfo)
    )

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[PartyId, PartyInfo]] = {

    def collectLatestByType[M <: TopologyMapping: ClassTag](
        storedTransactions: StoredTopologyTransactions[
          TopologyChangeOp.Replace,
          TopologyMapping,
        ],
        code: TopologyMapping.Code,
    ): Seq[M] =
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

    for {
      // get all party to participant mappings and also participant states for this uid (latter to mix in admin parties)
      partyData <- findTransactions(
        types = Seq(
          TopologyMapping.Code.PartyToParticipant,
          TopologyMapping.Code.SynchronizerTrustCertificate,
        ),
        filterUid = Some(parties.map(_.uid)),
        filterNamespace = None,
      ).map { storedTransactions =>
        // find normal party declarations
        val partyToParticipantMappings = collectLatestByType[PartyToParticipant](
          storedTransactions,
          TopologyMapping.Code.PartyToParticipant,
        ).map { ptp =>
          ptp.partyId -> (ptp.threshold, ptp.participants.map {
            case HostingParticipant(participantId, partyPermission) =>
              participantId -> partyPermission
          }.toMap)
        }.toMap

        // admin parties are implicitly defined by the fact that a participant is available on a synchronizer.
        // admin parties have the same UID as their participant
        val synchronizerTrustCerts = collectLatestByType[SynchronizerTrustCertificate](
          storedTransactions,
          TopologyMapping.Code.SynchronizerTrustCertificate,
        ).map(cert => cert.participantId)

        (partyToParticipantMappings, synchronizerTrustCerts)
      }
      (partyToParticipantMap, adminPartyParticipants) = partyData

      // fetch all admin parties
      participantIds = partyToParticipantMap.values.flatMap { case (_, participants) =>
        participants.keys
      }.toSeq ++ adminPartyParticipants

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
      // this can only affect participants that have left the synchronizer
      partiesToPartyInfos = {
        val p2pMappings = partyToParticipantMap.toSeq.mapFilter {
          case (partyId, (threshold, participantToPermissionsMap)) =>
            val participantIdToAttribs = participantToPermissionsMap.toSeq.mapFilter {
              case (participantId, partyPermission) =>
                participantToAttributesMap
                  .get(participantId)
                  .map { participantAttributes =>
                    // Use the lower permission between party and the permission granted to the participant by the synchronizer
                    val reducedPermission =
                      ParticipantPermission.lowerOf(
                        partyPermission,
                        participantAttributes.permission,
                      )
                    participantId -> ParticipantAttributes(
                      reducedPermission,
                      participantAttributes.loginAfter,
                    )
                  }
            }.toMap
            if (participantIdToAttribs.isEmpty) None
            else Some(partyId -> PartyInfo(threshold, participantIdToAttribs))
        }.toMap
        // In case of conflicting mappings, the admin party takes higher precedence
        // Note that conflicts are prevented in TopologyMappingChecks.
        p2pMappings ++ adminPartiesMap
      }
      // For each party we must return a result to satisfy the expectations of the
      // calling CachingTopologySnapshot's caffeine partyCache per findings in #11598.
      // This includes parties not found in the topology store or parties filtered out
      // above, e.g. parties whose participants have left the synchronizer.
      fullySpecifiedPartyMap = parties.map { party =>
        party -> partiesToPartyInfos.getOrElse(party, PartyInfo.EmptyPartyInfo)
      }.toMap
    } yield fullySpecifiedPartyMap
  }

  private def findMembersWithoutSigningKeys[T <: Member](members: Seq[T])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[T]] =
    signingKeys(members, SigningKeyUsage.All).map(keys =>
      members.filter(keys.get(_).forall(_.isEmpty)).toSet
    )

  /** returns the list of currently known mediator groups */
  override def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]] = {
    def fetchMediatorSynchronizerStates()
        : FutureUnlessShutdown[Seq[StoredTopologyTransaction[Replace, MediatorSynchronizerState]]] =
      findTransactions(
        types = Seq(TopologyMapping.Code.MediatorSynchronizerState),
        filterUid = None,
        filterNamespace = None,
      ).map(_.collectOfMapping[MediatorSynchronizerState].result)

    for {
      transactions <- fetchMediatorSynchronizerStates()
      mediatorsInAllGroups = transactions.flatMap(_.mapping.allMediatorsInGroup)
      mediatorsWithoutSigningKeys <- findMembersWithoutSigningKeys(mediatorsInAllGroups)
    } yield {
      transactions
        .groupBy(_.mapping.group)
        .map { case (groupId, seq) =>
          val mds = collectLatestMapping(
            TopologyMapping.Code.MediatorSynchronizerState,
            seq.sortBy(_.validFrom),
          )
            .getOrElse(
              throw new IllegalStateException("Group-by would not have produced empty seq")
            )
          MediatorGroup(
            groupId,
            mds.active.filterNot(mediatorsWithoutSigningKeys),
            mds.observers.filterNot(mediatorsWithoutSigningKeys),
            mds.threshold,
          )
        }
        .toSeq
        .sortBy(_.index)
    }
  }

  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]] = {
    def fetchSequencerSynchronizerState() = findTransactions(
      types = Seq(TopologyMapping.Code.SequencerSynchronizerState),
      filterUid = None,
      filterNamespace = None,
    ).map { transactions =>
      collectLatestMapping(
        TopologyMapping.Code.SequencerSynchronizerState,
        transactions.collectOfMapping[SequencerSynchronizerState].result,
      )
    }
    for {
      sds <- fetchSequencerSynchronizerState()
      allSequencers = sds.toList.flatMap(_.allSequencers)
      sequencersWithoutSigningKeys <- findMembersWithoutSigningKeys(allSequencers)
    } yield {
      sds.map { (sds: SequencerSynchronizerState) =>
        SequencerGroup(
          sds.active.filterNot(sequencersWithoutSigningKeys),
          sds.observers.filterNot(sequencersWithoutSigningKeys),
          sds.threshold,
        )
      }
    }
  }

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    store.inspectKnownParties(timestamp, filterParty, filterParticipant)

  /** Returns a list of owner's keys (at most limit) */
  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] = {
    val (idFilter, namespaceFilter) = UniqueIdentifier.splitFilter(filterOwner)
    store
      .inspect(
        proposals = false,
        timeQuery = TimeQuery.Snapshot(timestamp),
        asOfExclusiveO = None,
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
                if owner.filterString.startsWith(filterOwner) &&
                  filterOwnerType.forall(_ == owner.code) =>
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
    .collectOfMapping[SynchronizerTrustCertificate]
    .result
    .groupBy(_.mapping.participantId)
    .collect { case (pid, seq) =>
      // invoke collectLatestMapping only to warn in case a participantId's synchronizer trust certificate is not unique
      collectLatestMapping(
        TopologyMapping.Code.SynchronizerTrustCertificate,
        seq.sortBy(_.validFrom),
      ).discard
      pid
    }
    .toSet

  private def getParticipantsWithCertAndKeys(
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping],
      participantsWithCertificates: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Set[ParticipantId] =
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

  private def getParticipantSynchronizerPermissions(
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping],
      participantsWithCertAndKeys: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Map[ParticipantId, ParticipantSynchronizerPermission] =
    storedTxs
      .collectOfMapping[ParticipantSynchronizerPermission]
      .result
      .groupBy(_.mapping.participantId)
      .collect {
        case (pid, seq) if participantsWithCertAndKeys(pid) =>
          val mapping =
            collectLatestMapping(
              TopologyMapping.Code.ParticipantSynchronizerPermission,
              seq.sortBy(_.validFrom),
            )
              .getOrElse(
                throw new IllegalStateException("Group-by would not have produced empty seq")
              )
          pid -> mapping
      }

  private def loadParticipantStatesHelper(
      participantsFilter: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantSynchronizerPermission]] =
    for {
      // Looks up synchronizer parameters for default rate limits.
      synchronizerParametersState <- findTransactions(
        types = Seq(
          TopologyMapping.Code.SynchronizerParametersState
        ),
        filterUid = None,
        filterNamespace = None,
      ).map(transactions =>
        collectLatestMapping(
          TopologyMapping.Code.SynchronizerParametersState,
          transactions.collectOfMapping[SynchronizerParametersState].result,
        ).getOrElse(
          throw new IllegalStateException("Unable to locate synchronizer parameters state")
        )
      )
      storedTxs <- findTransactions(
        types = Seq(
          TopologyMapping.Code.SynchronizerTrustCertificate,
          TopologyMapping.Code.OwnerToKeyMapping,
          TopologyMapping.Code.ParticipantSynchronizerPermission,
        ),
        filterUid = Some(participantsFilter.map(_.uid)),
        filterNamespace = None,
      )

    } yield {
      // 1. Participant needs to have requested access to synchronizer by issuing a synchronizer trust certificate
      val participantsWithCertificates = getParticipantsWithCertificates(storedTxs)
      // 2. Participant needs to have keys registered on the synchronizer
      val participantsWithCertAndKeys =
        getParticipantsWithCertAndKeys(storedTxs, participantsWithCertificates)
      // Warn about participants with cert but no keys
      (participantsWithCertificates -- participantsWithCertAndKeys).foreach { pid =>
        logger.warn(
          s"Participant $pid has a synchronizer trust certificate, but no keys on synchronizer ${synchronizerParametersState.synchronizerId}"
        )
      }
      // 3. Attempt to look up permissions/trust from participant synchronizer permission
      val participantSynchronizerPermissions =
        getParticipantSynchronizerPermissions(storedTxs, participantsWithCertAndKeys)

      val participantIdSynchronizerPermissionsMap = participantsWithCertAndKeys.toSeq.mapFilter {
        pid =>
          if (
            synchronizerParametersState.parameters.onboardingRestriction.isRestricted && !participantSynchronizerPermissions
              .contains(pid)
          ) {
            // 4a. If the synchronizer is restricted, we must have found a ParticipantSynchronizerPermission for the participants, otherwise
            // the participants shouldn't have been able to onboard to the synchronizer in the first place.
            // In case we don't find a ParticipantSynchronizerPermission, we don't return the participant with default permissions, but we skip it.
            logger.warn(
              s"Unable to find ParticipantSynchronizerPermission for participant $pid on synchronizer ${synchronizerParametersState.synchronizerId} with onboarding restrictions ${synchronizerParametersState.parameters.onboardingRestriction} at $referenceTime"
            )
            None
          } else {
            // 4b. Apply default permissions/trust of submission/ordinary if missing participant synchronizer permission and
            // grab rate limits from dynamic synchronizer parameters if not specified
            Some(
              pid -> participantSynchronizerPermissions
                .getOrElse(
                  pid,
                  ParticipantSynchronizerPermission
                    .default(synchronizerParametersState.synchronizerId, pid),
                )
                .setDefaultLimitIfNotSet(
                  DynamicSynchronizerParameters.defaultParticipantSynchronizerLimits
                )
            )
          }
      }.toMap
      participantIdSynchronizerPermissionsMap
    }

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    if (participants.isEmpty)
      FutureUnlessShutdown.pure(Map())
    else
      loadParticipantStatesHelper(participants).map(_.map { case (pid, pdp) =>
        pid -> pdp.toParticipantAttributes
      })

  /** abstract loading function used to obtain the full key collection for a key owner */
  override def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection] =
    allKeys(Seq(owner)).map(_.getOrElse(owner, KeyCollection.empty))

  override def allKeys(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    findTransactions(
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

  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    findTransactions(
      types = Seq(
        SynchronizerTrustCertificate.code,
        MediatorSynchronizerState.code,
        SequencerSynchronizerState.code,
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.result.view
        .map(_.mapping)
        .flatMap {
          case dtc: SynchronizerTrustCertificate => Seq(dtc.participantId)
          case mds: MediatorSynchronizerState => mds.active ++ mds.observers
          case sds: SequencerSynchronizerState => sds.active ++ sds.observers
          case _ => Seq.empty
        }
        .toSet
    )

  override def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    areMembersKnown(Set(member)).map(_.nonEmpty)

  override def areMembersKnown(
      members: Set[Member]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[Member]] = {
    val participants = members.collect { case ParticipantId(uid) => uid }
    val mediators = members.collect { case MediatorId(uid) => uid }
    val sequencers = members.collect { case SequencerId(uid) => uid }

    val knownParticipantsF = if (participants.nonEmpty) {
      findTransactions(
        types = Seq(SynchronizerTrustCertificate.code),
        filterUid = Some(participants.toSeq),
        filterNamespace = None,
      ).map(
        _.collectOfMapping[SynchronizerTrustCertificate].result
          .map(_.mapping.participantId: Member)
          .toSet
      )
    } else FutureUnlessShutdown.pure(Set.empty[Member])

    val knownMediatorsF = if (mediators.nonEmpty) {
      findTransactions(
        types = Seq(MediatorSynchronizerState.code),
        filterUid = None,
        filterNamespace = None,
      ).map(
        _.collectOfMapping[MediatorSynchronizerState].result
          .flatMap(_.mapping.allMediatorsInGroup.collect {
            case med if mediators.contains(med.uid) => med: Member
          })
          .toSet
      )
    } else FutureUnlessShutdown.pure(Set.empty[Member])

    val knownSequencersF = if (sequencers.nonEmpty) {
      findTransactions(
        types = Seq(SequencerSynchronizerState.code),
        filterUid = None,
        filterNamespace = None,
      ).map(
        _.collectOfMapping[SequencerSynchronizerState].result
          .flatMap(_.mapping.allSequencers.collect {
            case seq if sequencers.contains(seq.uid) => seq: Member
          })
          .toSet
      )
    } else FutureUnlessShutdown.pure(Set.empty[Member])

    for {
      knownParticipants <- knownParticipantsF
      knownMediators <- knownMediatorsF
      knownSequencers <- knownSequencersF
    } yield knownParticipants ++ knownMediators ++ knownSequencers
  }

  override def memberFirstKnownAt(
      member: Member
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    member match {
      case participantId: ParticipantId =>
        store
          .findFirstTrustCertificateForParticipant(participantId)
          .map(_.map(tx => (tx.sequenced, tx.validFrom)))
      case mediatorId: MediatorId =>
        store
          .findFirstMediatorStateForMediator(mediatorId)
          .map(_.map(tx => (tx.sequenced, tx.validFrom)))
      case sequencerId: SequencerId =>
        store
          .findFirstSequencerStateForSequencer(sequencerId)
          .map(_.map(tx => (tx.sequenced, tx.validFrom)))
      case _ =>
        FutureUnlessShutdown.failed(
          new IllegalArgumentException(
            s"Checking whether member is known for an unexpected member type: $member"
          )
        )
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
    val result = transactions.maxByOption(_.validFrom)
    if (transactions.sizeIs > 1) {
      logger.error(
        show"Expected unique \"${typ.code}\" at $referenceTime, but found multiple instances. Using last one with serial ${result
            .map(_.serial)}"
      )
    }
    result
  }

  /** returns party authorization info for a party */
  override def partyAuthorization(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PartyKeyTopologySnapshotClient.PartyAuthorizationInfo]] =
    findTransactions(
      types = Seq(TopologyMapping.Code.PartyToKeyMapping),
      filterUid = Some(Seq(party.uid)),
      filterNamespace = None,
    ).map { transactions =>
      val keys = transactions
        .collectOfMapping[PartyToKeyMapping]
      keys.result.toList match {
        case head :: Nil =>
          val mapping = head.transaction.transaction.mapping
          Some(
            PartyKeyTopologySnapshotClient.PartyAuthorizationInfo(
              threshold = mapping.threshold,
              signingKeys = mapping.signingKeys,
            )
          )
        case Nil => None
        case _ => ErrorUtil.invalidState(s"Too many PartyToKeyMappings for $party: $keys")
      }
    }
}
