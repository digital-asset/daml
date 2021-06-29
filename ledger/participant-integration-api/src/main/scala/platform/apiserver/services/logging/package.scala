// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.Instant

import com.daml.ledger.api.domain.{
  ApplicationId,
  CommandId,
  Commands,
  EventId,
  LedgerId,
  LedgerOffset,
  TransactionFilter,
  TransactionId,
  WorkflowId,
}
import com.daml.logging.{LoggingEntries, LoggingEntry, LoggingValue}
import scalaz.syntax.tag.ToTagOps

package object logging {

  private[services] def parties(parties: Iterable[String]): LoggingEntry =
    "parties" -> parties

  private[services] def party(party: String): LoggingEntry =
    "parties" -> Seq(party)

  private[services] def actAs(parties: Iterable[String]): LoggingEntry =
    "actAs" -> parties

  private[services] def readAs(parties: Iterable[String]): LoggingEntry =
    "readAs" -> parties

  private[services] def startExclusive(offset: LedgerOffset): LoggingEntry =
    "startExclusive" -> offset

  private[services] def endInclusive(offset: Option[LedgerOffset]): LoggingEntry =
    "endInclusive" -> offset

  private[services] def offset(offset: Option[LedgerOffset]): LoggingEntry =
    "offset" -> offset

  private[services] def offset(offset: String): LoggingEntry =
    "offset" -> offset

  private[services] def ledgerId(id: LedgerId): LoggingEntry =
    "ledgerId" -> id.unwrap

  private[services] def applicationId(id: ApplicationId): LoggingEntry =
    "applicationId" -> id.unwrap

  private[services] def commandId(id: String): LoggingEntry =
    "commandId" -> id

  private[services] def commandId(id: CommandId): LoggingEntry =
    "commandId" -> id.unwrap

  private[services] def deduplicateUntil(instant: Instant): LoggingEntry =
    "deduplicateUntil" -> instant

  private[services] def eventId(id: EventId): LoggingEntry =
    "eventId" -> id.unwrap

  private[services] def filters(
      filters: TransactionFilter
  ): LoggingEntry =
    "filters" -> LoggingValue.Nested(
      LoggingEntries.fromMap(
        filters.filtersByParty.view
          .mapValues[LoggingValue](partyFilters =>
            partyFilters.inclusive match {
              case None => "all-templates"
              case Some(inclusiveFilters) => inclusiveFilters.templateIds.view.map(_.toString)
            }
          )
          .toMap
      )
    )

  private[services] def submissionId(id: String): LoggingEntry =
    "submissionId" -> id

  private[services] def submittedAt(t: Instant): LoggingEntry =
    "submittedAt" -> t

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

  private[services] def verbose(v: Boolean): LoggingEntry =
    "verbose" -> v

}
