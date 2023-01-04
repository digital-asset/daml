// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.ledger.api.benchtool.submission.{AllocatedParties, BenchtoolTestsPackageInfo}
import com.daml.ledger.client.binding.Primitive.TemplateId
import com.daml.ledger.test.benchtool.Foo.{Foo1, Foo2, Foo3}
import com.daml.ledger.test.benchtool.InterfaceSubscription.{FooI1, FooI2, FooI3}
import scalaz.syntax.tag._

class ConfigEnricher(
    allocatedParties: AllocatedParties,
    packageInfo: BenchtoolTestsPackageInfo,
) {

  private def toTemplateId[T](templateId: TemplateId[T]): (String, String) = {
    val id = templateId.unwrap
    id.entityName -> s"${packageInfo.packageId}:${id.moduleName}:${id.entityName}"
  }

  private val interfaceNameToFullyQualifiedNameMap: Map[String, String] = List(
    FooI1.id,
    FooI2.id,
    FooI3.id,
  ).map(toTemplateId).toMap

  private val templateNameToFullyQualifiedNameMap: Map[String, String] = List(
    Foo1.id,
    Foo2.id,
    Foo3.id,
  ).map(toTemplateId).toMap

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
    val convertedInterfaces = filter.interfaces.map(convertInterface)
    val convertedParties = convertPartySet(filter.partyNamePrefix)
    convertedParties.map(party =>
      PartyFilter(party = party, templates = convertedTemplates, interfaces = convertedInterfaces)
    )
  }

  private def convertPartySet(partyNamePrefix: String): List[String] = {
    val knownParties = allocatedParties.allAllocatedParties.map(_.unwrap)
    val matchedParties = knownParties.filter(_.startsWith(partyNamePrefix))
    if (matchedParties.isEmpty) {
      val partySetNames = knownParties.mkString(", ")
      sys.error(
        s"Expected party name prefix: '${partyNamePrefix}' does not match any of the known parties: $partySetNames"
      )
    } else
      matchedParties
  }

  private def enrichFilters(
      filters: List[StreamConfig.PartyFilter]
  ): List[StreamConfig.PartyFilter] = {
    filters.map { filter =>
      StreamConfig.PartyFilter(
        party = convertParty(filter.party),
        templates = filter.templates.map(convertTemplate),
        interfaces = filter.interfaces.map(convertInterface),
      )
    }
  }

  def convertTemplate(shortTemplateName: String): String =
    templateNameToFullyQualifiedNameMap.getOrElse(shortTemplateName, shortTemplateName)

  def convertInterface(shortInterfaceName: String): String =
    interfaceNameToFullyQualifiedNameMap.getOrElse(shortInterfaceName, shortInterfaceName)

}
