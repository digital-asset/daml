// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  ActiveContractsStreamConfig,
  CompletionsStreamConfig,
  PartyFilter,
  PartyNamePrefixFilter,
  TransactionLedgerEffectsStreamConfig,
  TransactionsStreamConfig,
}
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  AllocatedParties,
  BenchtoolTestsPackageInfo,
  FooTemplateDescriptor,
}

class ConfigEnricher(
    allocatedParties: AllocatedParties,
    packageInfo: BenchtoolTestsPackageInfo,
) {
  private val packageRef = packageInfo.packageRef.toString

  private def toTemplateId[T](templateId: Identifier): (String, String) =
    templateId.entityName -> s"$packageRef:${templateId.moduleName}:${templateId.entityName}"

  private val interfaceNameToFullyQualifiedNameMap: Map[String, String] = List(
    FooTemplateDescriptor.fooI1TemplateId(packageRef),
    FooTemplateDescriptor.fooI2TemplateId(packageRef),
    FooTemplateDescriptor.fooI3TemplateId(packageRef),
  ).map(toTemplateId).toMap

  private val templateNameToFullyQualifiedNameMap: Map[String, String] = List(
    FooTemplateDescriptor.Foo1(packageRef).templateId,
    FooTemplateDescriptor.Foo2(packageRef).templateId,
    FooTemplateDescriptor.Foo3(packageRef).templateId,
  ).map(toTemplateId).toMap

  def enrichStreamConfig(
      streamConfig: StreamConfig
  ): StreamConfig =
    streamConfig match {
      case config: TransactionsStreamConfig =>
        config
          .copy(
            filters = enrichFilters(config.filters) ++ config.partyNamePrefixFilters.flatMap(
              convertFilterByPartySet
            ),
            partyNamePrefixFilters = List.empty,
          )
      case config: TransactionLedgerEffectsStreamConfig =>
        config
          .copy(
            filters = enrichFilters(config.filters) ++ config.partyNamePrefixFilters.flatMap(
              convertFilterByPartySet
            ),
            partyNamePrefixFilters = List.empty,
          )
      case config: ActiveContractsStreamConfig =>
        config
          .copy(
            filters = enrichFilters(config.filters) ++ config.partyNamePrefixFilters.flatMap(
              convertFilterByPartySet
            ),
            partyNamePrefixFilters = List.empty,
          )
      case config: CompletionsStreamConfig =>
        config.copy(parties = config.parties.map(party => convertParty(party)))
    }

  private def convertParty(
      partyShortName: String
  ): String =
    allocatedParties.allAllocatedParties
      .map(_.getValue)
      .find(_.contains(partyShortName))
      .getOrElse(partyShortName)

  private def convertFilterByPartySet(
      filter: PartyNamePrefixFilter
  ): List[PartyFilter] = {
    val convertedTemplates = filter.templates.map(convertTemplate)
    val convertedInterfaces = filter.interfaces.map(convertInterface)
    val matchedParties = matchingParties(filter.partyNamePrefix)
    matchedParties.map(party =>
      PartyFilter(party = party, templates = convertedTemplates, interfaces = convertedInterfaces)
    )
  }

  private def matchingParties(partyNamePrefix: String): List[String] = {
    val knownParties = allocatedParties.allAllocatedParties.map(_.getValue)
    val matchedParties = knownParties.filter(_.startsWith(partyNamePrefix))
    if (matchedParties.isEmpty) {
      val knownPartiesText = knownParties.mkString(", ")
      sys.error(
        s"Expected party name prefix: '$partyNamePrefix' does not match any of the known parties: $knownPartiesText"
      )
    } else
      matchedParties
  }

  private def enrichFilters(
      filters: List[StreamConfig.PartyFilter]
  ): List[StreamConfig.PartyFilter] =
    filters.map { filter =>
      StreamConfig.PartyFilter(
        party = convertParty(filter.party),
        templates = filter.templates.map(convertTemplate),
        interfaces = filter.interfaces.map(convertInterface),
      )
    }

  def convertTemplate(shortTemplateName: String): String =
    templateNameToFullyQualifiedNameMap.getOrElse(shortTemplateName, shortTemplateName)

  def convertInterface(shortInterfaceName: String): String =
    interfaceNameToFullyQualifiedNameMap.getOrElse(shortInterfaceName, shortInterfaceName)

}
