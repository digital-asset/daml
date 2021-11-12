// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.submission.CommandSubmitter
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.test.model.Foo.{Foo1, Foo2, Foo3}
import scalaz.syntax.tag._

object ConfigEnricher {

  def enrichedStreamConfig(
      streamConfig: StreamConfig,
      submissionSummary: Option[CommandSubmitter.SubmissionSummary],
  ): StreamConfig = {
    streamConfig match {
      case config: StreamConfig.TransactionsStreamConfig =>
        config.copy(filters = enrichedFilters(config.filters, submissionSummary))
      case config: StreamConfig.TransactionTreesStreamConfig =>
        config.copy(filters = enrichedFilters(config.filters, submissionSummary))
      case config: StreamConfig.ActiveContractsStreamConfig =>
        config.copy(filters = enrichedFilters(config.filters, submissionSummary))
      case config: StreamConfig.CompletionsStreamConfig =>
        config.copy(party = convertedParty(config.party, submissionSummary))
    }
  }

  private def convertedParty(
      party: String,
      submissionSummary: Option[CommandSubmitter.SubmissionSummary],
  ): String =
    submissionSummary match {
      case None => party
      case Some(summary) =>
        summary.observers
          .map(_.unwrap)
          .find(_.contains(party))
          .getOrElse(throw new RuntimeException(s"Observer not found: $party"))
    }

  private def enrichedFilters(
      filters: List[StreamConfig.PartyFilter],
      submissionSummary: Option[CommandSubmitter.SubmissionSummary],
  ): List[StreamConfig.PartyFilter] = {
    def identifierToFullyQualifiedString(id: Identifier) =
      s"${id.packageId}:${id.moduleName}:${id.entityName}"
    def fullyQualifiedTemplateId(template: String): String =
      template match {
        case "Foo1" => identifierToFullyQualifiedString(Foo1.id.unwrap)
        case "Foo2" => identifierToFullyQualifiedString(Foo2.id.unwrap)
        case "Foo3" => identifierToFullyQualifiedString(Foo3.id.unwrap)
        case other => other
      }

    filters.map { filter =>
      StreamConfig.PartyFilter(
        party = convertedParty(filter.party, submissionSummary),
        templates = filter.templates.map(fullyQualifiedTemplateId),
      )
    }
  }

}
