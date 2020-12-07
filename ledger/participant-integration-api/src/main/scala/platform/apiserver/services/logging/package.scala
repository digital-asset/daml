// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  WorkflowId
}
import com.daml.ledger.api.v1.transaction_filter.Filters
import net.logstash.logback.argument.StructuredArguments
import scalaz.syntax.tag.ToTagOps

package object logging {

  private[services] def parties(parties: Iterable[String]): (String, String) =
    "parties" -> StructuredArguments.toString(parties.toArray)
  private[services] def party(party: String): (String, String) =
    "parties" -> StructuredArguments.toString(Array(party))
  private[services] def actAs(parties: Iterable[String]): (String, String) =
    "actAs" -> StructuredArguments.toString(parties.toArray)
  private[services] def readAs(parties: Iterable[String]): (String, String) =
    "readAs" -> StructuredArguments.toString(parties.toArray)
  private[services] def startExclusive(o: LedgerOffset): (String, String) =
    "startExclusive" -> offsetValue(o)
  private[services] def endInclusive(o: Option[LedgerOffset]): (String, String) =
    "endInclusive" -> nullableOffsetValue(o)
  private[services] def offset(o: Option[LedgerOffset]): (String, String) =
    "offset" -> nullableOffsetValue(o)
  private[this] def nullableOffsetValue(o: Option[LedgerOffset]): String =
    o.fold("")(offsetValue)
  private[this] def offsetValue(o: LedgerOffset): String =
    o match {
      case LedgerOffset.Absolute(absolute) => absolute
      case LedgerOffset.LedgerBegin => "%begin%"
      case LedgerOffset.LedgerEnd => "%end%"
    }
  private[services] def applicationId(id: ApplicationId): (String, String) =
    "applicationId" -> id.unwrap
  private[services] def commandId(id: String): (String, String) =
    "commandId" -> id
  private[services] def commandId(id: CommandId): (String, String) =
    "commandId" -> id.unwrap
  private[services] def deduplicateUntil(t: Instant): (String, String) =
    "deduplicateUntil" -> t.toString
  private[services] def eventId(id: EventId): (String, String) =
    "eventId" -> id.unwrap
  private[services] def filters(filtersByParty: Map[String, Filters]): Map[String, String] =
    filtersByParty.iterator.flatMap {
      case (party, filters) =>
        Iterator
          .continually(s"party-$party")
          .zip(filters.inclusive.fold(Iterator.single("all-templates"))(
            _.templateIds.iterator.map(_.toString)))
    }.toMap
  private[services] def submissionId(id: String): (String, String) =
    "submissionId" -> id
  private[services] def submittedAt(t: Instant): (String, String) =
    "submittedAt" -> t.toString
  private[services] def submitter(party: String): (String, String) =
    "submitter" -> party
  private[services] def transactionId(id: TransactionId): (String, String) =
    "transactionId" -> id.unwrap
  private[services] def workflowId(id: WorkflowId): (String, String) =
    "workflowId" -> id.unwrap
  private[services] def commands(cmds: Commands): Map[String, String] = {
    val context =
      Map(
        commandId(cmds.commandId),
        party(cmds.submitter),
        deduplicateUntil(cmds.deduplicateUntil),
        applicationId(cmds.applicationId),
        submittedAt(cmds.submittedAt),
        submitter(cmds.submitter),
      )
    cmds.workflowId.fold(context)(context + workflowId(_))
  }

}
