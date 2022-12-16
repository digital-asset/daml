// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.ledger.api.domain.{
  Commands,
  EventId,
  LedgerId,
  LedgerOffset,
  TransactionFilter,
  TransactionId,
}
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.data.logging._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.entries.LoggingValue.OfString
import com.daml.logging.entries.ToLoggingKey._
import com.daml.logging.entries.{LoggingEntries, LoggingEntry, LoggingValue}
import scalaz.syntax.tag.ToTagOps

package object logging {

  private[services] def parties(partyNames: Iterable[Party]): LoggingEntry =
    "parties" -> partyNames

  private[services] def partyStrings(partyNames: Iterable[String]): LoggingEntry =
    parties(partyNames.asInstanceOf[Iterable[Party]])

  private[services] def party(partyName: Party): LoggingEntry =
    "parties" -> Seq(partyName)

  private[services] def partyString(partyName: String): LoggingEntry =
    party(partyName.asInstanceOf[Party])

  private[services] def actAs(partyNames: Iterable[Party]): LoggingEntry =
    "actAs" -> partyNames

  private[services] def actAsStrings(partyNames: Iterable[String]): LoggingEntry =
    actAs(partyNames.asInstanceOf[Iterable[Party]])

  private[services] def readAs(partyNames: Iterable[Party]): LoggingEntry =
    "readAs" -> partyNames

  private[services] def readAsStrings(partyNames: Iterable[String]): LoggingEntry =
    readAs(partyNames.asInstanceOf[Iterable[Party]])

  private[services] def startExclusive(offset: LedgerOffset): LoggingEntry =
    "startExclusive" -> offset

  private[services] def endInclusive(offset: Option[LedgerOffset]): LoggingEntry =
    "endInclusive" -> offset

  private[services] def offset(offset: Option[LedgerOffset]): LoggingEntry =
    "offset" -> offset

  private[services] def offset(offset: String): LoggingEntry =
    "offset" -> offset

  private[services] def ledgerId(id: Option[LedgerId]): LoggingEntry =
    "ledgerId" -> OfString(id.map(_.unwrap).getOrElse("<empty-ledger-id>"))

  private[services] def commandId(id: String): LoggingEntry =
    "commandId" -> id

  private[services] def eventId(id: EventId): LoggingEntry =
    "eventId" -> id.unwrap

  private[services] def filters(
      filters: TransactionFilter
  ): LoggingEntry =
    "filters" -> LoggingValue.Nested(
      LoggingEntries.fromMap(
        filters.filtersByParty.view.map { case (party, partyFilters) =>
          party.toLoggingKey -> (partyFilters.inclusive match {
            case None => LoggingValue.from("all-templates")
            case Some(inclusiveFilters) => LoggingValue.from(inclusiveFilters.templateIds)
          })
        }.toMap
      )
    )

  private[services] def submissionId(id: String): LoggingEntry =
    "submissionId" -> id

  private[services] def transactionId(id: String): LoggingEntry =
    "transactionId" -> id

  private[services] def transactionId(id: TransactionId): LoggingEntry =
    "transactionId" -> id.unwrap

  private[services] def workflowId(id: String): LoggingEntry =
    "workflowId" -> id

  private[services] def packageId(id: String): LoggingEntry =
    "packageId" -> id

  private[services] def commands(cmds: Commands): LoggingEntry =
    "commands" -> cmds

  private[services] def verbose(v: Boolean): LoggingEntry =
    "verbose" -> v

  private[services] def contractId(id: ContractId): LoggingEntry =
    "contractId" -> id.coid

  private[services] def contractKey(key: Value): LoggingEntry =
    "contractKey" -> key.toString

  private[services] def templateId(id: Identifier): LoggingEntry =
    "templateId" -> id.toString

}
