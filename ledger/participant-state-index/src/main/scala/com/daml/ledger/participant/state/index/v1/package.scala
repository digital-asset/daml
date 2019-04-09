// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.value.Value

import scala.concurrent.Future

package object v1 {
  import com.daml.ledger.participant.state.v1._

  type TransactionAccepted = Update.TransactionAccepted
  type AsyncResult[T] = Future[Either[IndexService.Err, T]]

  /** The ledger offsets.
    * This extends the update identifier with further elements in order to
    * insert ephemeral rejection events into the event stream, while
    * retaining the ordering.
    */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class Offset(private val xs: Array[Int]) {
    override def toString: String =
      xs.mkString("-")

    def components: Iterable[Int] = xs
  }

  implicit object Offset extends Ordering[Offset] {

    def fromUpdateId(uId: UpdateId): Offset =
      Offset(uId.xs)

    /** Create an offset from a string of form 1-2-3. Throws
      * NumberFormatException on misformatted strings.
      */
    def assertFromString(s: String): Offset =
      Offset(s.split('-').map(_.toInt))

    override def compare(x: Offset, y: Offset): Int =
      scala.math.Ordering.Iterable[Int].compare(x.xs, y.xs)

  }

  /** ACS event identifier */
  type EventId = String

  final case class AcsUpdate(
      optSubmitterInfo: Option[SubmitterInfo],
      offset: Offset,
      transactionMeta: TransactionMeta,
      transactionId: String,
      events: List[AcsUpdateEvent]
  )

  sealed trait AcsUpdateEvent extends Product with Serializable
  object AcsUpdateEvent {
    final case class Create(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Identifier,
        argument: Value.VersionedValue[Value.AbsoluteContractId],
        // TODO(JM,SM): understand witnessing parties
        stakeholders: List[Party],
    ) extends AcsUpdateEvent

    final case class Archive(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Identifier,
        // TODO(JM,SM): understand witnessing parties
        stakeholders: List[Party],
    ) extends AcsUpdateEvent
  }

  sealed trait CompletionEvent extends Product with Serializable {
    def offset: Offset
  }
  object CompletionEvent {
    final case class Checkpoint(offset: Offset, recordTime: Timestamp) extends CompletionEvent
    // FIXME(JM): Remove offsets from these?
    final case class CommandAccepted(offset: Offset, commandId: String) extends CompletionEvent
    final case class CommandRejected(offset: Offset, commandId: String, reason: RejectionReason)
        extends CompletionEvent
  }

  final case class ActiveContractSetSnapshot(
      takenAt: Offset,
      activeContracts: Source[(WorkflowId, AcsUpdateEvent.Create), NotUsed])
}
