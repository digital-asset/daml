// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.Show
import com.digitalasset.canton.admin.api.client.data.CantonStatus.splitSuccessfulAndFailedStatus
import com.digitalasset.canton.console.{DomainReference, ParticipantReference}
import com.digitalasset.canton.health.admin.data.{DomainStatus, NodeStatus, ParticipantStatus}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.ShowUtil.*

trait CantonStatus extends PrettyPrinting {
  protected def descriptions[Status <: NodeStatus.Status](
      statusMap: Map[String, Status],
      failureMap: Map[String, NodeStatus.Failure],
      instanceType: String,
  ): Seq[String] = {

    val success = sort(statusMap)
      .map { case (d, status) =>
        show"Status for ${instanceType.unquoted} ${d.singleQuoted}:\n$status"
      }

    val failure = sort(failureMap)
      .map { case (d, status) =>
        show"${instanceType.unquoted} ${d.singleQuoted} cannot be reached: ${status.msg}"
      }

    success ++ failure
  }

  private def sort[K: Ordering, V](status: Map[K, V]): Seq[(K, V)] =
    status.toSeq.sortBy(_._1)
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
}

object CommunityCantonStatus {
  def getStatus(
      domains: Map[String, () => NodeStatus[DomainStatus]],
      participants: Map[String, () => NodeStatus[ParticipantStatus]],
  ): CommunityCantonStatus = {
    val (domainStatus, unreachableDomains) =
      splitSuccessfulAndFailedStatus(domains, DomainReference.InstanceType)
    val (participantStatus, unreachableParticipants) =
      splitSuccessfulAndFailedStatus(participants, ParticipantReference.InstanceType)

    CommunityCantonStatus(
      domainStatus,
      unreachableDomains,
      participantStatus,
      unreachableParticipants,
    )
  }
}

final case class CommunityCantonStatus(
    domainStatus: Map[String, DomainStatus],
    unreachableDomains: Map[String, NodeStatus.Failure],
    participantStatus: Map[String, ParticipantStatus],
    unreachableParticipants: Map[String, NodeStatus.Failure],
) extends CantonStatus {
  def tupled: (Map[String, DomainStatus], Map[String, ParticipantStatus]) =
    (domainStatus, participantStatus)

  override def pretty: Pretty[CommunityCantonStatus] = prettyOfString { _ =>
    val domains = descriptions(
      domainStatus,
      unreachableDomains,
      DomainReference.InstanceType,
    )
    val participants =
      descriptions(
        participantStatus,
        unreachableParticipants,
        ParticipantReference.InstanceType,
      )
    (domains ++ participants).mkString(System.lineSeparator() * 2)
  }
}
