// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.TestEnvironment
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarativeCommon.{
  External,
  OwningParticipant,
  PartyHostingState,
  PartyOwner,
  Serial,
}
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
sealed trait PartyToParticipantDeclarativeCommon[P] {
  def participants: Set[ParticipantReference]
  def synchronizerIds: Set[PhysicalSynchronizerId]
  def targetTopology: Map[
    P,
    Map[PhysicalSynchronizerId, PartyHostingState],
  ]

  def externalParties: Set[ExternalParty]

  def allRelevantParties: Set[P]

  protected def partyReference: PartyId => P

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
          .filter(result => allRelevantParties.contains(partyReference(result.item.partyId)))

        relevantResults.map { result =>
          val ptp = result.item

          (
            partyReference(ptp.partyId),
            synchronizerId,
            result.context.serial,
            ptp.threshold,
            ptp.participants.map(item => (item.participantId, item.permission)).toSet,
          )
        }
      }
      .groupMap { case (partyRef, _, _, _, _) => partyRef } {
        case (_, synchronizer, serial, threshold, permissions) =>
          synchronizer -> (serial, PartyHostingState(threshold, permissions))
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
  )

  def wrapIntoPartyHostingState(
      targetTopology: Map[
        PartyId,
        Map[PhysicalSynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
      ]
  ): Map[PartyId, Map[PhysicalSynchronizerId, PartyHostingState]] =
    targetTopology.view.mapValues(_.view.mapValues((PartyHostingState.apply _).tupled).toMap).toMap
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
    val externalParties: Set[ExternalParty],
    val targetTopology: Map[
      PartyId,
      Map[PhysicalSynchronizerId, PartyHostingState],
    ],
)(implicit executionContext: ExecutionContext, env: TestEnvironment)
    extends PartyToParticipantDeclarativeCommon[PartyId] {

  override protected def partyReference: PartyId => PartyId = identity

  override def allRelevantParties: Set[PartyId] =
    owningParticipants.keySet ++ externalParties.map(_.partyId)

  private def defaultParticipant: ParticipantReference = participants.headOption.getOrElse(
    throw new IllegalArgumentException("List of participants should not be empty")
  )

  private def getPartyOwner(partyId: PartyId): PartyOwner = externalParties
    .find(_.partyId == partyId) match {
    case Some(externalParty) => External(externalParty)
    case None => OwningParticipant(getOwningParticipantId(partyId))
  }

  private def flattenTarget(
      topology: Map[PartyId, Map[PhysicalSynchronizerId, PartyHostingState]]
  ): Map[
    (PartyId, PhysicalSynchronizerId),
    PartyHostingState,
  ] =
    topology.flatMap { case (partyId, topology) =>
      topology.map { case (synchronizer, hostingState) => (partyId, synchronizer) -> hostingState }
    }

  private def flattenCurrent(
      topology: Map[PartyId, Map[PhysicalSynchronizerId, (Serial, PartyHostingState)]]
  ): Map[
    (PartyId, PhysicalSynchronizerId),
    (Serial, PartyHostingState),
  ] =
    topology.flatMap { case (partyId, topology) =>
      topology.map { case (synchronizer, hostingState) => (partyId, synchronizer) -> hostingState }
    }

  def run(force: ForceFlags = ForceFlags.none): Unit = {
    sanityChecks()

    val target = flattenTarget(targetTopology)

    val current = flattenCurrent(getTopology(defaultParticipant)).view.mapValues {
      case (serial, hosting) => (Some(serial), hosting)
    }.toMap

    val result: Seq[Future[Unit]] = current.flatMap {
      case (
            (partyId, psid),
            (currentSerialO, PartyHostingState(currentThreshold, currentHosting)),
          ) =>
        val currentParticipantToPermission: Map[ParticipantId, ParticipantPermission] =
          currentHosting.toMap

        target.get((partyId, psid)) match {
          case Some(PartyHostingState(targetThreshold, targetHosting)) if targetHosting.nonEmpty =>
            val changeRequired =
              currentHosting != targetHosting || currentThreshold != targetThreshold

            if (changeRequired) {
              authorizeChange(
                partyId,
                psid,
                currentParticipantToPermission,
                targetThreshold,
                targetHosting = targetHosting,
                currentSerialO = currentSerialO,
                forceFlags = force,
              )
            } else Nil

          case _ => // removing the party from the synchronizer
            getPartyOwner(partyId) match {
              case OwningParticipant(owningParticipant) =>
                Seq(Future {
                  getParticipantReference(owningParticipant).topology.party_to_participant_mappings
                    .propose_delta(
                      partyId,
                      removes = currentParticipantToPermission.keySet.toSeq,
                      store = psid,
                      forceFlags = force,
                    )
                    .discard
                })

              case External(externalParty) =>
                Seq(Future {
                  defaultParticipant.topology.party_to_participant_mappings.sign_and_remove(
                    externalParty,
                    psid,
                  )
                })
            }
        }
    }.toSeq

    // For performance reasons, we delay synchronization until here
    Await.result(result.sequence, ConsoleCommandTimeout.defaultBoundedTimeout.asFiniteApproximation)

    val expectedTopology =
      targetTopology.view
        .mapValues(_.filter { case (_, hosting) => hosting.hosting.nonEmpty })
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
      partyId: PartyId,
      psid: PhysicalSynchronizerId,
      currentParticipantToPermission: Map[ParticipantId, ParticipantPermission],
      targetThreshold: PositiveInt,
      targetHosting: Set[(ParticipantId, ParticipantPermission)],
      currentSerialO: Option[Serial],
      forceFlags: ForceFlags,
  ): Seq[Future[Unit]] = {

    val newParticipants: Set[ParticipantId] = targetHosting.collect {
      case (participantId, targetPermissions)
          // permission of the participant is required when upgrading the hosting relationship
          if currentParticipantToPermission.get(participantId) < Some(targetPermissions) =>
        participantId
    }

    val partyOwner = getPartyOwner(partyId)
    val owningParticipant = partyOwner.toOwningParticipant.toList.toSet

    val authorizingParticipants = newParticipants ++ owningParticipant

    val participantAuthorizations = authorizingParticipants
      .map(getParticipantReference)
      .map { participant =>
        Future {
          participant.topology.party_to_participant_mappings
            .propose(
              party = partyId,
              newParticipants = targetHosting.toSeq,
              threshold = targetThreshold,
              store = psid,
              forceFlags = forceFlags,
              serial = Some(currentSerialO.fold(PositiveInt.one)(_.increment)),
            )
            .discard
        }
      }
      .toSeq

    // If the party is external, it needs to authorize the new mapping
    partyOwner.toExternal.foreach { externalParty =>
      val newPTP = createPTPMapping(partyId, psid, targetThreshold, targetHosting, currentSerialO)

      defaultParticipant.topology.transactions.load(
        Seq(env.global_secret.sign(newPTP, externalParty, psid.protocolVersion)),
        psid,
      )
    }

    participantAuthorizations
  }

  private def createPTPMapping(
      partyId: PartyId,
      psid: PhysicalSynchronizerId,
      targetThreshold: PositiveInt,
      targetHosting: Set[(ParticipantId, ParticipantPermission)],
      currentSerialO: Option[Serial],
  ): TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant] = TopologyTransaction(
    TopologyChangeOp.Replace,
    serial = currentSerialO.fold(PositiveInt.one)(_.increment),
    mapping = PartyToParticipant
      .create(
        partyId,
        threshold = targetThreshold,
        targetHosting.map { case (participant, permission) =>
          HostingParticipant(participant, permission, onboarding = false)
        }.toSeq,
      )
      .value,
    protocolVersion = psid.protocolVersion,
  )

  private def getOwningParticipantId(partyId: PartyId): ParticipantId =
    owningParticipants.getOrElse(
      partyId,
      throw new IllegalArgumentException(
        s"Party $partyId should be mentioned in owningParticipants"
      ),
    )

  private def sanityChecks(): Unit =
    targetTopology.keySet.foreach(partyId =>
      if (!owningParticipants.isDefinedAt(partyId) && isExternal(partyId).isEmpty)
        throw new IllegalArgumentException(
          s"Party $partyId should be mentioned in owningParticipants or should be external"
        )
    )
}

object PartyToParticipantDeclarative {
  def apply(
      participants: Set[ParticipantReference],
      synchronizerIds: Set[PhysicalSynchronizerId],
  )(
      owningParticipants: Map[PartyId, ParticipantId],
      targetTopology: Map[
        PartyId,
        Map[PhysicalSynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
      ],
      externalParties: Set[ExternalParty] = Set.empty,
      forceFlags: ForceFlags = ForceFlags.none,
  )(implicit executionContext: ExecutionContext, env: TestEnvironment): Unit =
    new PartyToParticipantDeclarative(participants, synchronizerIds)(
      owningParticipants,
      externalParties,
      targetTopology.view
        .mapValues(_.view.mapValues((PartyHostingState.apply _).tupled).toMap)
        .toMap,
    ).run(forceFlags)

  def forParty(
      participants: Set[ParticipantReference],
      synchronizerId: PhysicalSynchronizerId,
  )(
      owningParticipant: ParticipantId,
      partyId: PartyId,
      threshold: PositiveInt,
      hosting: Set[(ParticipantId, ParticipantPermission)],
      forceFlags: ForceFlags = ForceFlags.none,
  )(implicit executionContext: ExecutionContext, env: TestEnvironment): Unit =
    apply(participants, Set(synchronizerId))(
      Map(partyId -> owningParticipant),
      Map(partyId -> Map(synchronizerId -> (threshold, hosting))),
      Set.empty[ExternalParty],
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

  override protected def partyReference: PartyId => String = _.identifier.str

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
      (synchronizerId, PartyHostingState(threshold, hostingParticipants)) <- topology
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
        .mapValues(_.view.mapValues((PartyHostingState.apply _).tupled).toMap)
        .toMap,
    ).run()
}
