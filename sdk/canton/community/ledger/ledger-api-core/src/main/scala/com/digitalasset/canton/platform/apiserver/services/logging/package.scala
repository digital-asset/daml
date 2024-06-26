// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.logging.entries.LoggingValue.OfString
import com.daml.logging.entries.ToLoggingKey.*
import com.daml.logging.entries.{LoggingEntries, LoggingEntry, LoggingKey, LoggingValue}
import com.digitalasset.canton.ledger.api.domain.{
  Commands,
  CumulativeFilter,
  EventId,
  ParticipantOffset,
  TemplateWildcardFilter,
  TransactionFilter,
  TransactionId,
}
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.data.logging.*
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import scalaz.syntax.tag.ToTagOps

package object logging {

  private[services] def parties(partyNames: Iterable[Party]): LoggingEntry =
    "parties" -> partyNames

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[services] def partyStrings(partyNames: Iterable[String]): LoggingEntry =
    parties(partyNames.asInstanceOf[Iterable[Party]])

  private[services] def party(partyName: Party): LoggingEntry =
    "parties" -> Seq(partyName)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[services] def partyString(partyName: String): LoggingEntry =
    party(partyName.asInstanceOf[Party])

  private[services] def actAs(partyNames: Iterable[Party]): LoggingEntry =
    "actAs" -> partyNames

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[services] def actAsStrings(partyNames: Iterable[String]): LoggingEntry =
    actAs(partyNames.asInstanceOf[Iterable[Party]])

  private[services] def readAs(partyNames: Iterable[Party]): LoggingEntry =
    "readAs" -> partyNames

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[services] def readAsStrings(partyNames: Iterable[String]): LoggingEntry =
    readAs(partyNames.asInstanceOf[Iterable[Party]])

  private[services] def startExclusive(offset: ParticipantOffset): LoggingEntry =
    "startExclusive" -> offset

  private[services] def endInclusive(
      offset: Option[ParticipantOffset]
  ): LoggingEntry =
    "endInclusive" -> offset

  private[services] def offset(offset: String): LoggingEntry =
    "offset" -> offset

  private[services] def commandId(id: String): LoggingEntry =
    "commandId" -> id

  private[services] def eventSequentialId(seqId: Option[Long]): LoggingEntry =
    "eventSequentialId" -> OfString(seqId.map(_.toString).getOrElse("<empty-sequential-id>"))

  private[services] def eventId(id: EventId): LoggingEntry =
    "eventId" -> OfString(id.unwrap)

  private[services] def filters(
      filters: TransactionFilter
  ): LoggingEntry =
    "filters" -> LoggingValue.Nested(
      LoggingEntries.fromMap(
        filters.filtersByParty.view.map { case (party, partyFilters) =>
          party.toLoggingKey -> filtersToLoggingValue(partyFilters)
        }.toMap ++
          filters.filtersForAnyParty.fold(Map.empty[LoggingKey, LoggingValue])(filters =>
            Map("anyParty" -> filtersToLoggingValue(filters))
          )
      )
    )

  private def filtersToLoggingValue(filter: CumulativeFilter): LoggingValue =
    LoggingValue.Nested(
      LoggingEntries.fromMap(
        Map(
          "templates" -> LoggingValue.from(
            filter.templateFilters.map(_.templateTypeRef)
          ),
          "interfaces" -> LoggingValue.from(
            filter.interfaceFilters.map(_.interfaceId)
          ),
        )
          ++ (filter.templateWildcardFilter match {
            case Some(TemplateWildcardFilter(includeCreatedEventBlob)) =>
              Map(
                "all-templates, created_event_blob" -> LoggingValue.from(
                  includeCreatedEventBlob
                )
              )
            case None => Map.empty
          })
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
