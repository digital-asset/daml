// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{Filters, InterfaceFilter}
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.{
  InterfaceViewFilter,
  RenderResult,
}

/**  This class encapsulates the logic of how contract arguments and interface views are
  *  being projected to the consumer based on the filter criteria and the relation between
  *  interfaces and templates implementing them.
  *
  * @param verbose enriching in verbose mode
  * @param witnessTemplateIdFilter populate contract_argument, and contract_key. If templateId set is empty: populate.
  * @param witnessInterfaceViewFilter populate interface_views. The Map of templates to interfaces,
  *                              and the set of implementor templates cannot be empty.
  */
final case class EventProjectionProperties(
    verbose: Boolean,
    // Map(eventWitnessParty, Set(templateId))
    witnessTemplateIdFilter: Map[String, Set[Identifier]] = Map.empty,
    // Map(eventWitnessParty, Map(templateId -> Set(interfaceId)))
    witnessInterfaceViewFilter: Map[String, Map[Identifier, InterfaceViewFilter]] = Map.empty,
) {
  def render(witnesses: Set[String], templateId: Identifier): RenderResult = {
    val renderContractArguments: Boolean = witnesses.view
      .flatMap(witnessTemplateIdFilter.get)
      .exists(templates => templates.isEmpty || templates(templateId))

    val interfacesToRender: InterfaceViewFilter = witnesses.view
      .flatMap(witnessInterfaceViewFilter.get(_).iterator)
      .foldLeft(InterfaceViewFilter.Empty)(_.append(templateId, _))

    RenderResult(
      interfacesToRender.contractArgumentsBlob,
      renderContractArguments,
      interfacesToRender.interfaces,
    )
  }

}

object EventProjectionProperties {

  final case class InterfaceViewFilter(
      interfaces: Set[Identifier],
      contractArgumentsBlob: Boolean,
  ) {
    def append(
        templateId: Identifier,
        interfaceFilterMap: Map[Identifier, InterfaceViewFilter],
    ): InterfaceViewFilter = {
      val other = interfaceFilterMap.getOrElse(templateId, InterfaceViewFilter.Empty)
      InterfaceViewFilter(
        interfaces ++ other.interfaces,
        contractArgumentsBlob || other.contractArgumentsBlob,
      )
    }
  }

  object InterfaceViewFilter {
    val Empty = InterfaceViewFilter(Set.empty[Identifier], false)
  }

  final case class RenderResult(
      contractArgumentsBlob: Boolean,
      contractArguments: Boolean,
      interfaces: Set[Identifier],
  )

  /** @param transactionFilter     Transaction filter as defined by the consumer of the API.
    * @param verbose                enriching in verbose mode
    * @param interfaceImplementedBy The relation between an interface id and template id.
    *                               If template has no relation to the interface,
    *                               an empty Set must be returned.
    * @param alwaysPopulateArguments If this flag is set, the witnessTemplate filter will
    *                                be populated with all the parties, so that rendering of
    *                                contract arguments and contract keys is always true.
    */
  def apply(
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
      interfaceImplementedBy: Identifier => Set[Identifier],
      alwaysPopulateArguments: Boolean,
  ): EventProjectionProperties =
    EventProjectionProperties(
      verbose = verbose,
      witnessTemplateIdFilter = witnessTemplateIdFilter(transactionFilter, alwaysPopulateArguments),
      witnessInterfaceViewFilter =
        witnessInterfaceViewFilter(transactionFilter, interfaceImplementedBy),
    )

  private def witnessTemplateIdFilter(
      domainTransactionFilter: domain.TransactionFilter,
      alwaysPopulateArguments: Boolean,
  ): Map[String, Set[Identifier]] = {
    if (alwaysPopulateArguments)
      domainTransactionFilter.filtersByParty.keysIterator
        .map(_.toString -> Set.empty[Identifier])
        .toMap
    else
      domainTransactionFilter.filtersByParty.iterator
        .map { case (party, filters) => (party.toString, filters) }
        .collect {
          case (party, Filters(None)) => party -> Set.empty[Identifier]
          case (party, Filters(Some(empty)))
              if empty.templateIds.isEmpty && empty.interfaceFilters.isEmpty =>
            party -> Set.empty[Identifier]
          case (party, Filters(Some(nonEmptyFilter))) if nonEmptyFilter.templateIds.nonEmpty =>
            party -> nonEmptyFilter.templateIds
        }
        .toMap
  }

  private def witnessInterfaceViewFilter(
      domainTransactionFilter: domain.TransactionFilter,
      interfaceImplementedBy: Identifier => Set[Identifier],
  ): Map[String, Map[Identifier, InterfaceViewFilter]] = (for {
    (party, filters) <- domainTransactionFilter.filtersByParty.iterator
    inclusiveFilters <- filters.inclusive.iterator
    interfaceFilter <- inclusiveFilters.interfaceFilters.iterator
    implementor <- interfaceImplementedBy(interfaceFilter.interfaceId).iterator
  } yield (
    party,
    implementor,
    interfaceFilter,
  ))
    .toSet[(Party, Identifier, InterfaceFilter)]
    .groupMap(_._1) { case (_, templateId, interfaceFilter) =>
      templateId -> interfaceFilter
    }
    .map { case (partyId, templateAndInterfaceFilterPairs) =>
      (
        partyId.toString,
        templateAndInterfaceFilterPairs
          .groupMap(_._1)(_._2)
          .view
          .mapValues(interfaceFilters =>
            InterfaceViewFilter(
              interfaceFilters.filter(_.includeView).map(_.interfaceId),
              interfaceFilters.exists(_.includeCreateArgumentsBlob),
            )
          )
          .toMap,
      )
    }
}
