// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.domain.sequencing.sequencer.store.{Sequenced, SequencerMemberId}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.SortedSet
import scala.concurrent.Future

/** Who gets notified that a event has been written */
sealed trait WriteNotification {
  def union(notification: WriteNotification): WriteNotification
  def includes(memberId: SequencerMemberId): Boolean
}

object WriteNotification {

  case object None extends WriteNotification {
    override def union(notification: WriteNotification): WriteNotification = notification
    override def includes(memberId: SequencerMemberId): Boolean = false
  }
  final case class Members(memberIds: SortedSet[SequencerMemberId]) extends WriteNotification {
    override def union(notification: WriteNotification): WriteNotification =
      notification match {
        case Members(newMemberIds) => Members(memberIds ++ newMemberIds)
        case None => this
      }

    override def includes(memberId: SequencerMemberId): Boolean = memberIds.contains(memberId)

    override def toString: String = s"Members(${memberIds.map(_.unwrap).mkString(",")})"
  }

  def apply(events: NonEmpty[Seq[Sequenced[_]]]): WriteNotification =
    events
      .map(_.event.notifies)
      .reduceLeft(_ union _)
}

/** Signal that a reader should attempt to read the latest events as some may have been written */
sealed trait ReadSignal
case object ReadSignal extends ReadSignal

/** Component to signal to a [[SequencerReader]] that more events may be available to read so should attempt
  * fetching events from its store.
  */
trait EventSignaller extends AutoCloseable {
  def notifyOfLocalWrite(notification: WriteNotification)(implicit
      traceContext: TraceContext
  ): Future[Unit]
  def readSignalsForMember(member: Member, memberId: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): Source[ReadSignal, NotUsed]
}
