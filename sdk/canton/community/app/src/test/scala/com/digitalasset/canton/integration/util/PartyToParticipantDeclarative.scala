// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import org.scalatest.Assertions.fail

import scala.concurrent.{Await, ExecutionContext, Future}

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
  def synchronizerIds: Set[SynchronizerId]
  def targetTopology: Map[
    P,
    Map[SynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
  ]

  protected def partyReference: PartyId => P

  protected val participantReferences: Map[ParticipantId, ParticipantReference] =
    participants.map(p => p.id -> p).toMap

  protected def getParticipantReference(id: ParticipantId): ParticipantReference =
    participantReferences.getOrElse(
      id,
      throw new IllegalArgumentException(s"Unknown participant with id $id"),
    )

  protected def retryUntil(f: => Boolean): Unit =
    BaseTest.eventually() {
      if (f) () else fail()
    }

  protected def getTopology(
      participant: ParticipantReference
  ): Map[P, Map[SynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])]] =
    synchronizerIds
      .flatMap { synchronizerId =>
        val relevantPtp = participant.topology.party_to_participant_mappings
          .list(synchronizerId)
          .map(_.item)
          .filter(item => targetTopology.contains(partyReference(item.partyId)))

        relevantPtp.map { ptp =>
          (
            partyReference(ptp.partyId),
            synchronizerId,
            ptp.threshold,
            ptp.participants.map(item => (item.participantId, item.permission)).toSet,
          )
        }
      }
      .groupMap { case (partyRef, _, _, _) => partyRef } {
        case (_, synchronizer, threshold, permissions) =>
          synchronizer -> (threshold, permissions)
      }
      .view
      .mapValues(_.toMap)
      .toMap
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
    val synchronizerIds: Set[SynchronizerId],
)(
    owningParticipants: Map[PartyId, ParticipantId],
    val targetTopology: Map[
      PartyId,
      Map[SynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
    ],
)(implicit executionContext: ExecutionContext)
    extends PartyToParticipantDeclarativeCommon[PartyId] {

  override protected def partyReference: PartyId => PartyId = identity

  private def defaultParticipant: ParticipantReference = participants.headOption.getOrElse(
    throw new IllegalArgumentException("List of participants should not be empty")
  )

  private def flatten(
      topology: Map[
        PartyId,
        Map[SynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
      ]
  ): Map[(PartyId, SynchronizerId), (PositiveInt, Set[(ParticipantId, ParticipantPermission)])] =
    topology.flatMap { case (partyId, topology) =>
      topology.map { case (synchronizer, partyInfo) => (partyId, synchronizer) -> partyInfo }
    }

  def run(force: ForceFlags = ForceFlags.none): Unit = {
    sanityChecks()

    val current = flatten(getTopology(defaultParticipant))
    val target = flatten(targetTopology)

    val result: Seq[Future[Unit]] = current.flatMap {
      case ((partyId, synchronizerId), (currentThreshold, currentHosting)) =>
        val owningParticipant = getParticipantReference(getOwningParticipantId(partyId))
        val currentParticipantToPermission: Map[ParticipantId, ParticipantPermission] =
          currentHosting.toMap

        target.get((partyId, synchronizerId)) match {
          case Some((targetThreshold, targetHosting)) =>
            val changeRequired =
              currentHosting != targetHosting || currentThreshold != targetThreshold
            if (changeRequired) {
              val authorizationRequired = targetHosting.collect {
                case (participantId, _targetPermissions)
                    // permission of the participant is only required for onboarding
                    if !currentParticipantToPermission.contains(participantId) =>
                  participantId

              } + getOwningParticipantId(partyId)

              authorizationRequired
                .map(getParticipantReference)
                .map { participant =>
                  Future {
                    participant.topology.party_to_participant_mappings
                      .propose(
                        party = partyId,
                        newParticipants = targetHosting.toSeq,
                        threshold = targetThreshold,
                        store = synchronizerId,
                        forceFlags = force,
                      )
                      .discard
                  }
                }
                .toSeq
            } else Nil

          case None => // removing the party from the synchronizer
            Seq(Future {
              owningParticipant.topology.party_to_participant_mappings
                .propose_delta(
                  partyId,
                  removes = currentParticipantToPermission.keySet.toSeq,
                  store = synchronizerId,
                  forceFlags = force,
                )
                .discard
            })
        }
    }.toSeq

    // For performance reasons, we delay synchronization until here
    Await.result(result.sequence, ConsoleCommandTimeout.defaultBoundedTimeout.asFiniteApproximation)

    participants.toSeq.foreach(p => retryUntil(getTopology(p) == targetTopology))
  }

  private def getOwningParticipantId(partyId: PartyId): ParticipantId =
    owningParticipants.getOrElse(
      partyId,
      throw new IllegalArgumentException(
        s"Party $partyId should be mentioned in owningParticipants"
      ),
    )

  private def sanityChecks(): Unit =
    targetTopology.keySet.foreach(partyId =>
      if (!owningParticipants.isDefinedAt(partyId))
        throw new IllegalArgumentException(
          s"Party $partyId should be mentioned in owningParticipants"
        )
    )
}

object PartyToParticipantDeclarative {
  def apply(
      participants: Set[ParticipantReference],
      synchronizerIds: Set[SynchronizerId],
  )(
      owningParticipants: Map[PartyId, ParticipantId],
      targetTopology: Map[
        PartyId,
        Map[SynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
      ],
      forceFlags: ForceFlags = ForceFlags.none,
  )(implicit executionContext: ExecutionContext): Unit =
    new PartyToParticipantDeclarative(participants, synchronizerIds)(
      owningParticipants,
      targetTopology,
    ).run(forceFlags)

  def forParty(
      participants: Set[ParticipantReference],
      synchronizerId: SynchronizerId,
  )(
      owningParticipant: ParticipantId,
      partyId: PartyId,
      threshold: PositiveInt,
      hosting: Set[(ParticipantId, ParticipantPermission)],
      forceFlags: ForceFlags = ForceFlags.none,
  )(implicit executionContext: ExecutionContext): Unit =
    apply(participants, Set(synchronizerId))(
      Map(partyId -> owningParticipant),
      Map(partyId -> Map(synchronizerId -> (threshold, hosting))),
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
    val targetTopology: Map[
      String,
      Map[SynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
    ],
)(implicit executionContext: ExecutionContext)
    extends PartyToParticipantDeclarativeCommon[String] {

  override def synchronizerIds: Set[SynchronizerId] = targetTopology.values.flatMap(_.keys).toSet

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
      (synchronizerId, (threshold, hostingParticipants)) <- topology
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
        p.synchronizers.list_connected().map(_.synchronizerId.logical).toSet
      val topologyOnConnectedSynchronizers =
        targetTopology.view
          .mapValues(_.view.filterKeys(connectedSynchronizers.contains).toMap)
          .filter { case (_keys, values) => values.nonEmpty }
          .toMap

      retryUntil(getTopology(p) == topologyOnConnectedSynchronizers)
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
        Map[SynchronizerId, (PositiveInt, Set[(ParticipantId, ParticipantPermission)])],
      ],
  )(implicit executionContext: ExecutionContext): Seq[PartyId] =
    new PartiesAllocator(participants)(newParties, targetTopology).run()

}
