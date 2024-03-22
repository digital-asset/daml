// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.LedgerString.ordering
import com.daml.lf.value.Value as Lf
import scalaz.syntax.tag.*
import scalaz.{@@, Tag}

package object domain {
  type Value = Lf

  type WorkflowId = Ref.WorkflowId @@ WorkflowIdTag
  val WorkflowId: Tag.TagOf[WorkflowIdTag] = Tag.of[WorkflowIdTag]

  type CommandId = Ref.CommandId @@ CommandIdTag
  val CommandId: Tag.TagOf[CommandIdTag] = Tag.of[CommandIdTag]

  type TransactionId = Ref.TransactionId @@ TransactionIdTag
  val TransactionId: Tag.TagOf[TransactionIdTag] = Tag.of[TransactionIdTag]

  type EventId = Ref.LedgerString @@ EventIdTag
  val EventId: Tag.TagOf[EventIdTag] = Tag.of[EventIdTag]
  implicit val eventIdOrdering: Ordering[EventId] =
    Ordering.by[EventId, Ref.LedgerString](_.unwrap)

  type LedgerId = String @@ LedgerIdTag
  val LedgerId: Tag.TagOf[LedgerIdTag] = Tag.of[LedgerIdTag]

  def optionalLedgerId(raw: String): Option[LedgerId] =
    if (raw.isEmpty) None else Some(LedgerId(raw))

  type ParticipantId = Ref.ParticipantId @@ ParticipantIdTag
  val ParticipantId: Tag.TagOf[ParticipantIdTag] = Tag.of[ParticipantIdTag]

  type SubmissionId = Ref.SubmissionId @@ SubmissionIdTag
  val SubmissionId: Tag.TagOf[SubmissionIdTag] = Tag.of[SubmissionIdTag]
}

package domain {
  sealed trait WorkflowIdTag
  sealed trait CommandIdTag
  sealed trait TransactionIdTag
  sealed trait EventIdTag
  sealed trait LedgerIdTag
  sealed trait ParticipantIdTag
  sealed trait SubmissionIdTag
}
