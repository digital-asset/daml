// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.test.model.Foo.{Foo1, Foo2, Foo3}
import scalaz.syntax.tag._

object ConfigEnricher {

  def enrichStreamConfig(
      streamConfig: StreamConfig,
      submissionResult: Option[SubmissionStepResult],
  ): StreamConfig = {
    streamConfig match {
      case config: StreamConfig.TransactionsStreamConfig =>
        config.copy(filters = enrichFilters(config.filters, submissionResult))
      case config: StreamConfig.TransactionTreesStreamConfig =>
        config.copy(filters = enrichFilters(config.filters, submissionResult))
      case config: StreamConfig.ActiveContractsStreamConfig =>
        config.copy(filters = enrichFilters(config.filters, submissionResult))
      case config: StreamConfig.CompletionsStreamConfig =>
        config.copy(parties = config.parties.map(party => convertParty(party, submissionResult)))
    }
  }

  private def convertParty(
      party: String,
      submissionResult: Option[SubmissionStepResult],
  ): String =
    submissionResult match {
      case None => party
      case Some(summary) =>
        summary.allocatedParties.allAllocatedParties
          .map(_.unwrap)
          .find(_.contains(party))
          .getOrElse(throw new RuntimeException(s"Observer not found: $party"))
    }

  private def enrichFilters(
      filters: List[StreamConfig.PartyFilter],
      submissionResult: Option[SubmissionStepResult],
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
        party = convertParty(filter.party, submissionResult),
        templates = filter.templates.map(fullyQualifiedTemplateId),
      )
    }
  }

}
