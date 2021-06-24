// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.Instant

import com.daml.ledger.api.domain.{
  ApplicationId,
  CommandId,
  Commands,
  EventId,
  LedgerOffset,
  TransactionId,
  WorkflowId,
}
import com.daml.ledger.api.v1.transaction_filter.Filters
import com.daml.logging.{LoggingEntries, LoggingEntry}
import net.logstash.logback.argument.StructuredArguments
import scalaz.syntax.tag.ToTagOps

package object logging {

  private[services] def parties(parties: Iterable[String]): LoggingEntry =
    "parties" -> StructuredArguments.toString(parties.toArray)

  private[services] def party(party: String): LoggingEntry =
    "parties" -> StructuredArguments.toString(Array(party))

  private[services] def actAs(parties: Iterable[String]): LoggingEntry =
    "actAs" -> StructuredArguments.toString(parties.toArray)

  private[services] def readAs(parties: Iterable[String]): LoggingEntry =
    "readAs" -> StructuredArguments.toString(parties.toArray)

  private[services] def startExclusive(o: LedgerOffset): LoggingEntry =
    "startExclusive" -> offsetValue(o)

  private[services] def endInclusive(o: Option[LedgerOffset]): LoggingEntry =
    "endInclusive" -> nullableOffsetValue(o)

  private[services] def offset(o: Option[LedgerOffset]): LoggingEntry =
    "offset" -> nullableOffsetValue(o)

  private[this] def nullableOffsetValue(o: Option[LedgerOffset]): String =
    o.fold("")(offsetValue)

  private[this] def offsetValue(o: LedgerOffset): String =
    o match {
      case LedgerOffset.Absolute(absolute) => absolute
      case LedgerOffset.LedgerBegin => "%begin%"
      case LedgerOffset.LedgerEnd => "%end%"
    }

  private[services] def applicationId(id: ApplicationId): LoggingEntry =
    "applicationId" -> id.unwrap

  private[services] def commandId(id: String): LoggingEntry =
    "commandId" -> id

  private[services] def commandId(id: CommandId): LoggingEntry =
    "commandId" -> id.unwrap

  private[services] def deduplicateUntil(t: Instant): LoggingEntry =
    "deduplicateUntil" -> t.toString

  private[services] def eventId(id: EventId): LoggingEntry =
    "eventId" -> id.unwrap

  private[services] def filters(filtersByParty: Map[String, Filters]): LoggingEntries =
    LoggingEntries.fromIterator(filtersByParty.iterator.flatMap { case (party, filters) =>
      Iterator
        .continually(s"party-$party")
        .zip(
          filters.inclusive.fold(Iterator.single("all-templates"))(
            _.templateIds.iterator.map(_.toString)
          )
        )
    })

  private[services] def submissionId(id: String): LoggingEntry =
    "submissionId" -> id

  private[services] def submittedAt(t: Instant): LoggingEntry =
    "submittedAt" -> t.toString

  private[services] def transactionId(id: String): LoggingEntry =
    "transactionId" -> id

  private[services] def transactionId(id: TransactionId): LoggingEntry =
    "transactionId" -> id.unwrap

  private[services] def workflowId(id: String): LoggingEntry =
    "workflowId" -> id

  private[services] def workflowId(id: WorkflowId): LoggingEntry =
    "workflowId" -> id.unwrap

  private[services] def commands(cmds: Commands): LoggingEntries = {
    val context = LoggingEntries(
      commandId(cmds.commandId),
      deduplicateUntil(cmds.deduplicateUntil),
      applicationId(cmds.applicationId),
      submittedAt(cmds.submittedAt),
      actAs(cmds.actAs),
      readAs(cmds.readAs),
    )
    cmds.workflowId.fold(context)(context :+ workflowId(_))
  }

}
