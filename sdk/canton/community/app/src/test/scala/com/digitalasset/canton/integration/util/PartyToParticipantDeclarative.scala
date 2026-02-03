// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.crypto.SigningKeysWithThreshold
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarativeCommon.{
  PartyHostingState,
  Serial,
}
import com.digitalasset.canton.integration.{PartyTopologyUtils, TestEnvironment}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyTransaction,
}
import org.scalatest.Assertions.fail
import org.scalatest.EitherValues.*

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.math.Ordering.Implicits.*

/*
There are two classes extending this trait:
- PartiesAllocator
  Deals with parties that are not yet allocated. In particular, parties are designated
  by their name (type parameter P=String).

- PartyToParticipantDeclarative
  Deals with parties that are already allocated. In particular, parties are designated
  by their id (type parameter P=PartyId).
 */
sealed trait PartyToParticipantDeclarativeCommon[P] extends PartyTopologyUtils {
  def participants: Set[ParticipantReference]
  def synchronizerIds: Set[PhysicalSynchronizerId]
  def targetTopology: Map[
    P,
    Map[PhysicalSynchronizerId, PartyHostingState],
  ]

  def externalParties: Set[ExternalParty] = targetTopology.keys.collect {
    case externalParty: ExternalParty => externalParty
  }.toSet

  def allRelevantParties: Set[P]

  protected def partyReference: PartyToParticipant => P

  protected val participantReferences: Map[ParticipantId, ParticipantReference] =
    participants.map(p => p.id -> p).toMap

  def isExternal(partyId: PartyId): Option[ExternalParty] =
    externalParties.find(_.partyId == partyId)

  protected def getParticipantReference(id: ParticipantId): ParticipantReference =
    participantReferences.getOrElse(
      id,
      throw new IllegalArgumentException(s"Unknown participant with id $id"),
    )

  protected def retryUntil[A](expected: A)(actual: => A): Unit =
    BaseTest.eventually() {
      if (actual == expected) () else fail(s"""Couldn't reach target topology:
           |expected: $expected
           |actual  : $actual""".stripMargin)
    }

  protected def getTopology(
      participant: ParticipantReference
  ): Map[P, Map[PhysicalSynchronizerId, (Serial, PartyHostingState)]] =
    synchronizerIds
      .flatMap { synchronizerId =>
        val relevantResults = participant.topology.party_to_participant_mappings
          .list(synchronizerId.logical)
          .filter(result => allRelevantParties.contains(partyReference(result.item)))

        relevantResults.map { result =>
          val ptp = result.item
          val party = partyReference(ptp)

          (
            party,
            synchronizerId,
            result.context.serial,
            ptp.threshold,
            ptp.participants.map(item => (item.participantId, item.permission)).toSet,
            ptp.partySigningKeysWithThreshold,
          )
        }
      }
      .groupMap { case (partyRef, _, _, _, _, _) => partyRef } {
        case (_, synchronizer, serial, threshold, permissions, partySigningKeys) =>
          synchronizer -> (serial, PartyHostingState(threshold, permissions, partySigningKeys))
      }
      .view
      .mapValues(_.toMap)
      .toMap
}

object PartyToParticipantDeclarativeCommon {
  type Serial = PositiveInt

  sealed trait PartyOwner extends Product with Serializable {
    def toOwningParticipant: Option[ParticipantId]
    def toExternal: Option[ExternalParty]
  }
  final case class OwningParticipant(p: ParticipantId) extends PartyOwner {
    override def toOwningParticipant: Option[ParticipantId] = Some(p)
    override def toExternal: Option[ExternalParty] = None
  }
  final case class External(party: ExternalParty) extends PartyOwner {
    override def toOwningParticipant: Option[ParticipantId] = None
    override def toExternal: Option[ExternalParty] = Some(party)
  }

  final case class PartyHostingState(
      confirmationThreshold: PositiveInt,
      hosting: Set[(ParticipantId, ParticipantPermission)],
      partySigningKeys: Option[SigningKeysWithThreshold],
  )
}

/** Attempt to allocate the specified parties and try to reach the target topology.
  *
  * @param participants
  *   Participants used to issue transactions. Synchronization is done on those participants.
  * @param synchronizerIds
  *   Synchronizers in scope
  * @param owningParticipants
  *   For each party mentioned in the target topology, owning participant
  * @param targetTopology
  *   For each party, the required target state
  * @param onboarding
  *   For each added participant in the target state, whether to specify the onboarding flag
  *
  * Preconditions:
  *   - `participants` is non-empty
  *   - Each participant in `participants` is connected to all the `synchronizers`
  *
  * Some methods in this class are unsafe. This is fine in tests.
  */
class PartyToParticipantDeclarative(
    val participants: Set[ParticipantReference],
    val synchronizerIds: Set[PhysicalSynchronizerId],
)(
    owningParticipants: Map[PartyId, ParticipantId],
    val targetTopology: Map[
      Party,
      Map[PhysicalSynchronizerId, PartyHostingState],
    ],
    onboarding: Boolean,
)(implicit executionContext: ExecutionContext, env: TestEnvironment)
    extends PartyToParticipantDeclarativeCommon[Party] {

  override protected def partyReference: PartyToParticipant => Party = ptp => {
    ptp.partySigningKeysWithThreshold match {
      case Some(SigningKeysWithThreshold(keys, threshold)) =>
        ExternalParty(ptp.partyId, keys.map(_.fingerprint).toSeq, threshold)
      case None => ptp.partyId
    }
  }

  override def allRelevantParties: Set[Party] =
    owningParticipants.keySet ++ externalParties

  private def defaultParticipant: ParticipantReference = participants.headOption.getOrElse(
    throw new IllegalArgumentException("List of participants should not be empty")
  )

  private def flattenTarget(
      topology: Map[Party, Map[PhysicalSynchronizerId, PartyHostingState]]
  ): Map[
    (Party, PhysicalSynchronizerId),
    PartyHostingState,
  ] =
    topology.flatMap { case (partyId, topology) =>
      topology.map { case (synchronizer, hostingState) => (partyId, synchronizer) -> hostingState }
    }

  private def flattenCurrent(
      topology: Map[Party, Map[PhysicalSynchronizerId, (Serial, PartyHostingState)]]
  ): Map[
    (Party, PhysicalSynchronizerId),
    (Serial, PartyHostingState),
  ] =
    topology.flatMap { case (party, topology) =>
      topology.map { case (synchronizer, hostingState) => (party, synchronizer) -> hostingState }
    }

  def run(force: ForceFlags = ForceFlags.none): Unit = {
    sanityChecks()

    val target = flattenTarget(targetTopology)

    val current = flattenCurrent(getTopology(defaultParticipant)).view.mapValues {
      case (serial, hosting) => (Some(serial), hosting)
    }.toMap

    val result: Seq[Future[Unit]] = current.flatMap {
      case (
            (party, psid),
            (
              currentSerialO,
              PartyHostingState(currentThreshold, currentHosting, currentPartySigningKeys),
            ),
          ) =>
        val currentParticipantToPermission: Map[ParticipantId, ParticipantPermission] =
          currentHosting.toMap

        target.get((party, psid)) match {
          case Some(PartyHostingState(targetThreshold, targetHosting, targetPartySigningKeys))
              if targetHosting.nonEmpty =>
            val changeRequired =
              currentHosting != targetHosting || currentThreshold != targetThreshold || currentPartySigningKeys != targetPartySigningKeys

            if (changeRequired) {
              authorizeChange(
                party,
                psid,
                currentParticipantToPermission,
                targetThreshold,
                targetHosting = targetHosting,
                currentSerialO = currentSerialO,
                forceFlags = force,
                targetPartySigningKeys = targetPartySigningKeys,
              )
            } else Nil

          case _ => // removing the party from the synchronizer
            Seq(Future {
              party.topology.party_to_participant_mappings
                .propose_delta(
                  owningParticipants
                    .get(party.partyId)
                    .map(getParticipantReference)
                    .getOrElse(defaultParticipant),
                  removes = currentParticipantToPermission.keySet.toSeq,
                  store = psid,
                  forceFlags = force,
                )
                .discard
            })
        }
    }.toSeq

    // For performance reasons, we delay synchronization until here
    Await.result(result.sequence, ConsoleCommandTimeout.defaultBoundedTimeout.asFiniteApproximation)

    val expectedTopology =
      targetTopology.view
        .mapValues(_.filter { case (_, hosting) => hosting.hosting.nonEmpty })
        .filter { case (_, hosting) => hosting.nonEmpty }
        .toMap

    participants.toSeq.foreach { p =>
      retryUntil(expectedTopology) {
        getTopology(p).view
          .mapValues(_.view.mapValues { case (_serial, hosting) => hosting }.toMap)
          .toMap
      }
    }
  }

  private def authorizeChange(
      party: Party,
      psid: PhysicalSynchronizerId,
      currentParticipantToPermission: Map[ParticipantId, ParticipantPermission],
      targetThreshold: PositiveInt,
      targetHosting: Set[(ParticipantId, ParticipantPermission)],
      currentSerialO: Option[Serial],
      forceFlags: ForceFlags,
      targetPartySigningKeys: Option[SigningKeysWithThreshold],
  ): Seq[Future[Unit]] = {
    val newParticipants: Set[ParticipantId] = targetHosting.collect {
      case (participantId, targetPermissions)
          // permission of the participant is required when upgrading the hosting relationship
          if currentParticipantToPermission.get(participantId) < Some(targetPermissions) =>
        participantId
    }

    val owningParticipant = getOwningParticipantIdO(party).toList.toSet
    val authorizingParticipants = newParticipants ++ owningParticipant
    val onboardingParticipants =
      if (onboarding) (targetHosting.map { case (pid, _) =>
        pid
      } -- currentParticipantToPermission.keySet)
      else Set.empty[ParticipantId]

    val participantAuthorizations = authorizingParticipants
      .map(getParticipantReference)
      .map { participant =>
        Future {
          participant.topology.party_to_participant_mappings
            .propose(
              party = party.partyId,
              newParticipants = targetHosting.toSeq,
              threshold = targetThreshold,
              store = psid,
              forceFlags = forceFlags,
              serial = Some(currentSerialO.fold(PositiveInt.one)(_.increment)),
              partySigningKeys = targetPartySigningKeys,
              participantsRequiringPartyToBeOnboarded = onboardingParticipants.toSeq,
            )
            .discard
        }
      }
      .toSeq

    // If the party is external, it needs to authorize the new mapping
    party match {
      case externalParty: ExternalParty =>
        val newPTP = createPTPMapping(
          externalParty.partyId,
          psid,
          targetThreshold,
          targetHosting,
          currentSerialO,
          targetPartySigningKeys,
          onboardingParticipants,
        )

        defaultParticipant.topology.transactions.load(
          Seq(env.global_secret.sign(newPTP, externalParty, psid.protocolVersion)),
          psid,
        )
      case _ =>
    }

    participantAuthorizations
  }

  private def createPTPMapping(
      partyId: PartyId,
      psid: PhysicalSynchronizerId,
      targetThreshold: PositiveInt,
      targetHosting: Set[(ParticipantId, ParticipantPermission)],
      currentSerialO: Option[Serial],
      signingKeysWithThreshold: Option[SigningKeysWithThreshold],
      onboardingParticipants: Set[ParticipantId],
  ): TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant] = TopologyTransaction(
    TopologyChangeOp.Replace,
    serial = currentSerialO.fold(PositiveInt.one)(_.increment),
    mapping = PartyToParticipant
      .create(
        partyId,
        threshold = targetThreshold,
        targetHosting.map { case (participant, permission) =>
          HostingParticipant(
            participant,
            permission,
            onboarding = onboardingParticipants.contains(participant),
          )
        }.toSeq,
        partySigningKeysWithThreshold = signingKeysWithThreshold,
      )
      .value,
    protocolVersion = psid.protocolVersion,
  )

  private def getOwningParticipantIdO(party: Party): Option[ParticipantId] = party match {
    case _: ExternalParty => None
    case partyId: PartyId => Some(getOwningParticipantId(partyId))
  }

  private def getOwningParticipantId(partyId: PartyId): ParticipantId =
    owningParticipants.getOrElse(
      partyId,
      throw new IllegalArgumentException(
        s"Party $partyId should be mentioned in owningParticipants"
      ),
    )

  private def sanityChecks(): Unit =
    targetTopology.keySet.foreach {
      case partyId: PartyId =>
        if (!owningParticipants.isDefinedAt(partyId)) {
          throw new IllegalArgumentException(
            s"Local party $partyId should be mentioned in owningParticipants"
          )
        }
      case _ =>
    }
}

object PartyToParticipantDeclarative {
  def apply(
      participants: Set[ParticipantReference],
      synchronizerIds: Set[PhysicalSynchronizerId],
  )(
      owningParticipants: Map[PartyId, ParticipantId],
      targetTopology: Map[
        Party,
        Map[PhysicalSynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
      ],
      forceFlags: ForceFlags = ForceFlags.none,
      onboarding: Boolean = false, // participants added in target topology are marked as onboarding
  )(implicit executionContext: ExecutionContext, env: TestEnvironment): Unit = {
    val participantReference = participants.headOption.getOrElse(
      fail("No participant set in PartyToParticipantDeclarative")
    )
    val signingKeysPerPartyPerSynchronizer = targetTopology.values
      .flatMap(_.keySet)
      .map { synchronizer =>
        synchronizer ->
          participantReference.topology.party_to_participant_mappings
            .list(synchronizer)
            .flatMap(result =>
              result.item.partySigningKeysWithThreshold.map(result.item.partyId -> _)
            )
            .toMap
      }
      .toMap

    new PartyToParticipantDeclarative(participants, synchronizerIds)(
      owningParticipants,
      targetTopology.map {
        // For external parties, lookup their signing keys from the synchronizer's topology
        case (external: ExternalParty, target) =>
          external -> target.view.map { case (synchronizer, (threshold, hosting)) =>
            val signingKeys =
              signingKeysPerPartyPerSynchronizer.get(synchronizer).flatMap(_.get(external.partyId))
            synchronizer -> PartyHostingState(threshold, hosting, signingKeys)
          }.toMap

        case (partyId: PartyId, target) =>
          partyId -> target.view
            .mapValues { case (threshold, hosting) =>
              (threshold, hosting, Option.empty[SigningKeysWithThreshold])
            }
            .mapValues((PartyHostingState.apply _).tupled)
            .toMap
      },
      onboarding,
    ).run(forceFlags)
  }

  def forParty(
      participants: Set[ParticipantReference],
      synchronizerId: PhysicalSynchronizerId,
  )(
      owningParticipant: ParticipantId,
      party: Party,
      threshold: PositiveInt,
      hosting: Set[(ParticipantId, ParticipantPermission)],
      forceFlags: ForceFlags = ForceFlags.none,
  )(implicit executionContext: ExecutionContext, env: TestEnvironment): Unit =
    apply(participants, Set(synchronizerId))(
      Map(party.partyId -> owningParticipant),
      Map(party -> Map(synchronizerId -> (threshold, hosting))),
      forceFlags,
    )
}

/** Attempt to allocate the specified parties and try to reach the target topology.
  *
  * @param participants
  *   Participants used to issue transactions. Synchronization is done on those participants.
  * @param synchronizerIds
  *   Synchronizers in scope
  * @param newParties
  *   Parties to be allocated
  * @param targetTopology
  *   For each party, the required target state
  *
  * Preconditions:
  *   - `participants` is non-empty
  *   - Each participant in `participants` is connected to all the `synchronizers`
  *
  * Some methods in this class are unsafe. This is fine in tests.
  */
class PartiesAllocator(
    val participants: Set[ParticipantReference]
)(
    newParties: Seq[(String, ParticipantId)],
    val targetTopology: Map[String, Map[PhysicalSynchronizerId, PartyHostingState]],
)(implicit executionContext: ExecutionContext)
    extends PartyToParticipantDeclarativeCommon[String] {

  override def externalParties: Set[ExternalParty] = Set.empty

  override def synchronizerIds: Set[PhysicalSynchronizerId] =
    targetTopology.values.flatMap(_.keys).toSet

  override def allRelevantParties: Set[String] = newParties.map { case (name, _) => name }.toSet

  override protected def partyReference: PartyToParticipant => String = _.partyId.identifier.str

  def run(): Seq[PartyId] = {
    val result = for {
      (partyName, owningParticipantId) <- newParties
      owningParticipant = getParticipantReference(owningParticipantId)
      topology = targetTopology.getOrElse(
        partyName,
        throw new IllegalArgumentException(
          s"Party $partyName should be mentioned in target topology state"
        ),
      )
      partyId = UniqueIdentifier
        .create(partyName, owningParticipant.namespace)
        .map(PartyId(_))
        .valueOr(err =>
          throw new RuntimeException(s"Unable to create party id for $partyName: $err")
        )
      (synchronizerId, PartyHostingState(threshold, hostingParticipants, _partySigningKeys)) <-
        topology
      hostingParticipantIds = hostingParticipants.map { case (id, _) => id }
      authorizingParticipantsId = hostingParticipantIds + owningParticipantId
      authorizingParticipants = authorizingParticipantsId.map(getParticipantReference)
      res = authorizingParticipants.map { authorizingParticipant =>
        Future {
          authorizingParticipant.topology.party_to_participant_mappings
            .propose(
              party = partyId,
              newParticipants = hostingParticipants.toSeq,
              threshold = threshold,
              store = synchronizerId,
            )
            .discard
        }
      }
    } yield partyId -> res

    // For performance reasons, we delay synchronization until here
    Await.result(
      result.flatMap(_._2).sequence,
      ConsoleCommandTimeout.defaultBoundedTimeout.asFiniteApproximation,
    )

    // synchronization
    participants.toSeq.foreach { p =>
      val connectedSynchronizers =
        p.synchronizers.list_connected().map(_.physicalSynchronizerId).toSet
      val topologyOnConnectedSynchronizers =
        targetTopology.view
          .mapValues(_.view.filterKeys(connectedSynchronizers.contains).toMap)
          .filter { case (_keys, values) => values.nonEmpty }
          .toMap

      retryUntil(topologyOnConnectedSynchronizers) {
        getTopology(p).view
          .mapValues(_.view.mapValues { case (_serial, hosting) => hosting }.toMap)
          .toMap
      }
    }
    result.map(_._1)
  }
}

object PartiesAllocator {
  def apply(
      participants: Set[ParticipantReference]
  )(
      newParties: Seq[(String, ParticipantId)],
      targetTopology: Map[
        String,
        Map[PhysicalSynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
      ],
  )(implicit executionContext: ExecutionContext): Seq[PartyId] =
    new PartiesAllocator(participants)(
      newParties,
      targetTopology.view
        .mapValues(
          _.view
            // TODO(i29008): Support onboarding of external parties by setting SigningKeysWithThreshold
            .mapValues { case (threshold, hosting) =>
              (threshold, hosting, Option.empty[SigningKeysWithThreshold])
            }
            .mapValues((PartyHostingState.apply _).tupled)
            .toMap
        )
        .toMap,
    ).run()
}
