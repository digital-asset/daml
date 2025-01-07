// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.Show
import com.digitalasset.canton.console.{MediatorReference, ParticipantReference, SequencerReference}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.ShowUtil.*

final case class CantonStatus(
    sequencerStatus: Map[String, SequencerStatus],
    unreachableSequencers: Map[String, NodeStatus.Failure],
    mediatorStatus: Map[String, MediatorStatus],
    unreachableMediators: Map[String, NodeStatus.Failure],
    participantStatus: Map[String, ParticipantStatus],
    unreachableParticipants: Map[String, NodeStatus.Failure],
) extends PrettyPrinting {
  protected def descriptions[Status <: NodeStatus.Status](
      statusMap: Map[String, Status],
      failureMap: Map[String, NodeStatus.Failure],
      instanceType: String,
  ): Seq[String] = {

    val success = sort(statusMap)
      .map { case (d, status) =>
        show"Status for ${instanceType.unquoted} ${d.singleQuoted}:${System.lineSeparator()}$status"
      }

    val failure = sort(failureMap)
      .map { case (d, status) =>
        show"${instanceType.unquoted} ${d.singleQuoted} cannot be reached: ${status.msg}"
      }

    success ++ failure
  }

  private def sort[K: Ordering, V](status: Map[K, V]): Seq[(K, V)] =
    status.toSeq.sortBy(_._1)

  override protected def pretty: Pretty[CantonStatus] = prettyOfString { status =>
    val sequencers = descriptions(
      status.sequencerStatus,
      status.unreachableSequencers,
      SequencerReference.InstanceType,
    )
    val mediators = descriptions(
      status.mediatorStatus,
      status.unreachableMediators,
      MediatorReference.InstanceType,
    )
    val participants =
      descriptions(
        status.participantStatus,
        status.unreachableParticipants,
        ParticipantReference.InstanceType,
      )
    (sequencers ++ mediators ++ participants).mkString(System.lineSeparator() * 2)
  }
}

object CantonStatus {
  def splitSuccessfulAndFailedStatus[K: Show, S <: NodeStatus.Status](
      nodes: Map[K, () => NodeStatus[S]],
      instanceType: String,
  ): (Map[K, S], Map[K, NodeStatus.Failure]) = {
    val map: Map[K, NodeStatus[S]] =
      nodes.map { case (node, getStatus) =>
        node -> getStatus()
      }
    val status: Map[K, S] =
      map.collect { case (n, NodeStatus.Success(status)) =>
        n -> status
      }
    val unreachable: Map[K, NodeStatus.Failure] =
      map.collect {
        case (s, entry: NodeStatus.Failure) => s -> entry
        case (s, _: NodeStatus.NotInitialized) =>
          s -> NodeStatus.Failure(
            s"${instanceType.unquoted} ${s.show.singleQuoted} has not been initialized"
          )
      }
    (status, unreachable)
  }

  def getStatus(
      sequencers: Map[String, () => NodeStatus[SequencerStatus]],
      mediators: Map[String, () => NodeStatus[MediatorStatus]],
      participants: Map[String, () => NodeStatus[ParticipantStatus]],
  ): CantonStatus = {
    val (sequencerStatus, unreachableSequencers) =
      splitSuccessfulAndFailedStatus(sequencers, SequencerReference.InstanceType)
    val (mediatorStatus, unreachableMediators) =
      splitSuccessfulAndFailedStatus(mediators, MediatorReference.InstanceType)
    val (participantStatus, unreachableParticipants) =
      splitSuccessfulAndFailedStatus(participants, ParticipantReference.InstanceType)

    CantonStatus(
      sequencerStatus,
      unreachableSequencers,
      mediatorStatus,
      unreachableMediators,
      participantStatus,
      unreachableParticipants,
    )
  }
}
