// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  ActiveContractsStreamConfig,
  CompletionsStreamConfig,
  PartyFilter,
  PartyNamePrefixFilter,
  TransactionTreesStreamConfig,
  TransactionsStreamConfig,
}
import com.daml.ledger.api.benchtool.submission.AllocatedParties
import com.daml.ledger.test.benchtool.Foo.{Foo1, Foo2, Foo3}
import scalaz.syntax.tag._

class ConfigEnricher(allocatedParties: AllocatedParties) {

  private val templateNameToFullyQualifiedNameMap: Map[String, String] = List(
    Foo1.id,
    Foo2.id,
    Foo3.id,
  ).map { templateId =>
    val id = templateId.unwrap
    id.entityName -> s"${id.packageId}:${id.moduleName}:${id.entityName}"
  }.toMap

  def enrichStreamConfig(
      streamConfig: StreamConfig
  ): StreamConfig = {
    streamConfig match {
      case config: TransactionsStreamConfig =>
        config
          .copy(
            filters = enrichFilters(config.filters) ++ convertFilterByPartySet(
              config.partyNamePrefixFilterO
            ),
            partyNamePrefixFilterO = None,
          )
      case config: TransactionTreesStreamConfig =>
        config
          .copy(
            filters = enrichFilters(config.filters) ++ convertFilterByPartySet(
              config.partyNamePrefixFilterO
            ),
            partyNamePrefixFilterO = None,
          )
      case config: ActiveContractsStreamConfig =>
        config
          .copy(
            filters = enrichFilters(config.filters) ++ convertFilterByPartySet(
              config.partyNamePrefixFilterO
            ),
            partyNamePrefixFilterO = None,
          )
      case config: CompletionsStreamConfig =>
        config.copy(parties = config.parties.map(party => convertParty(party)))
    }
  }

  private def convertParty(
      partyShortName: String
  ): String =
    allocatedParties.allAllocatedParties
      .map(_.unwrap)
      .find(_.contains(partyShortName))
      .getOrElse(partyShortName)

  private def convertFilterByPartySet(
      filter: Option[PartyNamePrefixFilter]
  ): List[PartyFilter] =
    filter.fold(List.empty[PartyFilter])(convertFilterByPartySet)

  private def convertFilterByPartySet(
      filter: PartyNamePrefixFilter
  ): List[PartyFilter] = {
    val convertedTemplates = filter.templates.map(convertTemplate)
    val convertedParties = convertPartySet(filter.partyNamePrefix)
    convertedParties.map(party => PartyFilter(party = party, templates = convertedTemplates))
  }

  private def convertPartySet(partySetName: String): List[String] =
    allocatedParties.observerPartySetO match {
      case None =>
        sys.error(
          "Cannot desugar party-set-template-filter as observer party set allocation is missing"
        )
      case Some(observerPartySet) =>
        if (observerPartySet.partyNamePrefix == partySetName)
          observerPartySet.parties.map(_.unwrap)
        else
          sys.error(
            s"Expected party set: '${partySetName}' does not match actual party set: ${observerPartySet.partyNamePrefix}"
          )
    }

  private def enrichFilters(
      filters: List[StreamConfig.PartyFilter]
  ): List[StreamConfig.PartyFilter] = {
    filters.map { filter =>
      StreamConfig.PartyFilter(
        party = convertParty(filter.party),
        templates = filter.templates.map(convertTemplate),
      )
    }
  }

  def convertTemplate(shortTemplateName: String): String =
    templateNameToFullyQualifiedNameMap.getOrElse(shortTemplateName, shortTemplateName)

}
