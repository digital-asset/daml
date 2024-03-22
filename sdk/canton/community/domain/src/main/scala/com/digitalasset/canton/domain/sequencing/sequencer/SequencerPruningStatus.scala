// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequired}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{Member, UnauthenticatedMemberId}

final case class SequencerMemberStatus(
    member: Member,
    registeredAt: CantonTimestamp,
    lastAcknowledged: Option[CantonTimestamp],
    enabled: Boolean = true,
) extends PrettyPrinting {
  def safePruningTimestamp: CantonTimestamp =
    lastAcknowledged.getOrElse(registeredAt)

  def toProtoV0: v0.SequencerMemberStatus =
    v0.SequencerMemberStatus(
      member.toProtoPrimitive,
      Some(registeredAt.toProtoPrimitive),
      lastAcknowledged.map(_.toProtoPrimitive),
      enabled,
    )

  override def pretty: Pretty[SequencerMemberStatus] = prettyOfClass(
    param("member", _.member),
    param("registered at", _.registeredAt),
    paramIfDefined("last acknowledged", _.lastAcknowledged),
    paramIfTrue("enabled", _.enabled),
  )
}

/** Structure housing both members and instances of those members. Used to list clients that have been or need to be
  * disabled.
  */
final case class SequencerClients(
    members: Set[Member] = Set.empty
)

trait AbstractSequencerPruningStatus {

  /** the earliest timestamp that can be read */
  def lowerBound: CantonTimestamp

  /** details of registered members */
  def members: Seq[SequencerMemberStatus]

  lazy val disabledClients: SequencerClients = SequencerClients(
    members = members.filterNot(_.enabled).map(_.member).toSet
  )

  /** Using the member details, calculate based on their acknowledgements when is the latest point we can
    * safely prune without losing any data that may still be read.
    *
    * @param timestampForNoMembers The timestamp to return if there are no unignored members
    */
  def safePruningTimestampFor(timestampForNoMembers: CantonTimestamp): CantonTimestamp = {
    val earliestMemberTs = members.filter(_.enabled).map(_.safePruningTimestamp).minOption
    earliestMemberTs.getOrElse(timestampForNoMembers)
  }
}

private[canton] final case class InternalSequencerPruningStatus(
    override val lowerBound: CantonTimestamp,
    override val members: Seq[SequencerMemberStatus],
) extends AbstractSequencerPruningStatus
    with PrettyPrinting {
  def toSequencerPruningStatus(now: CantonTimestamp): SequencerPruningStatus =
    SequencerPruningStatus(lowerBound, now, members)

  override def pretty: Pretty[InternalSequencerPruningStatus] = prettyOfClass(
    param("lower bound", _.lowerBound),
    param("members", _.members),
  )
}

private[canton] object InternalSequencerPruningStatus {

  /** Sentinel value to use for Sequencers that don't yet support the status endpoint */
  val Unimplemented =
    InternalSequencerPruningStatus(CantonTimestamp.MinValue, members = Seq.empty)

}

/** Pruning status of a Sequencer.
  *
  * @param now the current time of the sequencer clock
  */
final case class SequencerPruningStatus(
    override val lowerBound: CantonTimestamp,
    now: CantonTimestamp,
    override val members: Seq[SequencerMemberStatus],
) extends AbstractSequencerPruningStatus
    with PrettyPrinting {

  def toInternal: InternalSequencerPruningStatus =
    InternalSequencerPruningStatus(lowerBound, members)

  /** Using the member details, calculate based on their acknowledgements when is the latest point we can
    * safely prune without losing any data that may still be read.
    *
    * if there are no members (or they've all been ignored), we can technically prune everything.
    * as in practice a domain will register a IDM, Sequencer and Mediator, this will most likely never occur.
    */
  lazy val safePruningTimestamp: CantonTimestamp = safePruningTimestampFor(now)

  def unauthenticatedMembersToDisable(retentionPeriod: NonNegativeFiniteDuration): Set[Member] =
    members.foldLeft(Set.empty[Member]) { (toDisable, memberStatus) =>
      memberStatus.member match {
        case _: UnauthenticatedMemberId if memberStatus.enabled =>
          if (now.minus(retentionPeriod.unwrap) > memberStatus.safePruningTimestamp) {
            toDisable + memberStatus.member
          } else toDisable
        case _ => toDisable
      }
    }

  /** List clients that would need to be disabled to allow pruning at the given timestamp.
    */
  def clientsPreventingPruning(timestamp: CantonTimestamp): SequencerClients =
    members.foldLeft(SequencerClients()) { (disabled, memberStatus) =>
      if (memberStatus.safePruningTimestamp.isBefore(timestamp)) {
        disabled.copy(members = disabled.members + memberStatus.member)
      } else disabled
    }

  def toProtoV0: v0.SequencerPruningStatus =
    v0.SequencerPruningStatus(
      earliestEventTimestamp = Some(lowerBound.toProtoPrimitive),
      now = Some(now.toProtoPrimitive),
      members = members.map(_.toProtoV0),
    )

  override def pretty: Pretty[SequencerPruningStatus] = prettyOfClass(
    param("lower bound", _.lowerBound),
    param("now", _.now),
    paramIfNonEmpty("members", _.members),
  )
}

object SequencerMemberStatus {
  def fromProtoV0(
      memberStatusP: v0.SequencerMemberStatus
  ): ParsingResult[SequencerMemberStatus] =
    for {
      member <- Member.fromProtoPrimitive(memberStatusP.member, "member")
      registeredAt <- parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "registeredAt",
        memberStatusP.registeredAt,
      )
      lastAcknowledgedO <- memberStatusP.lastAcknowledged.traverse(
        CantonTimestamp.fromProtoPrimitive
      )
    } yield SequencerMemberStatus(member, registeredAt, lastAcknowledgedO, memberStatusP.enabled)
}

object SequencerPruningStatus {

  /** Sentinel value to use for Sequencers that don't yet support the status endpoint */
  lazy val Unimplemented =
    SequencerPruningStatus(CantonTimestamp.MinValue, CantonTimestamp.MinValue, members = Seq.empty)

  def fromProtoV0(
      statusP: v0.SequencerPruningStatus
  ): ParsingResult[SequencerPruningStatus] =
    for {
      earliestEventTimestamp <- parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "earliestEventTimestamp",
        statusP.earliestEventTimestamp,
      )
      now <- parseRequired(CantonTimestamp.fromProtoPrimitive, "now", statusP.now)
      members <- statusP.members.traverse(SequencerMemberStatus.fromProtoV0)
    } yield SequencerPruningStatus(earliestEventTimestamp, now, members)
}
