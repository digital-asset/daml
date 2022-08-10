// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.Filters
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.platform.store.dao.EventProjectionProperties.RenderResult

/** @param verbose enriching in verbose mode
  * @param populateContractArgument populate contract_argument, and contract_key. If templateId set is empty: populate.
  * @param populateInterfaceView populate interface_views. The Map of templates to interfaces,
  *                              and the set of implementor templates cannot be empty.
  */
final case class EventProjectionProperties private[dao] (
    verbose: Boolean,
    // Map(eventWitnessParty, Set(templateId))
    populateContractArgument: Map[String, Set[Identifier]] = Map.empty,
    // Map(eventWitnessParty, Map(templateId -> Set(interfaceId)))
    populateInterfaceView: Map[String, Map[Identifier, Set[Identifier]]] = Map.empty,
) {
  def render(witnesses: Seq[String], templateId: Identifier): RenderResult = {
    val renderContractArguments = witnesses.view
      .flatMap(populateContractArgument.get)
      .exists(templates => templates.isEmpty || templates(templateId))

    val renderInterfaces: Set[Identifier] =
      (for {
        witness <- witnesses.iterator
        templateToInterfaceMap <- populateInterfaceView.get(witness).iterator
        interfaces <- templateToInterfaceMap.getOrElse(templateId, Set.empty[Identifier])
      } yield interfaces).toSet

    RenderResult(renderContractArguments, renderInterfaces)
  }

}

object EventProjectionProperties {

  case class RenderResult(
      contractArguments: Boolean,
      interfaces: Set[Identifier],
  )

  /** @param domainTransactionFilter Transaction filter as defined by the consumer of the API.
    * @param verbose                 enriching in verbose mode
    * @param interfaceImplementedBy  The relation between an interface id and template id.
    *                                If template has no relation to the interface,
    *                                an empty Set must be returned.
    */
  def apply(
      domainTransactionFilter: domain.TransactionFilter,
      verbose: Boolean,
      interfaceImplementedBy: Identifier => Set[Identifier],
  ): EventProjectionProperties =
    EventProjectionProperties(
      verbose = verbose,
      populateContractArgument = populateContractArgument(domainTransactionFilter),
      populateInterfaceView = populateInterfaceView(domainTransactionFilter, interfaceImplementedBy),
    )

  private def populateContractArgument(
      domainTransactionFilter: domain.TransactionFilter
  ): Map[String, Set[Identifier]] = {

    val wildcardFilters = domainTransactionFilter.filtersByParty.iterator
      .collect {
        case (party, Filters(None)) => party
        case (party, Filters(Some(empty)))
            if empty.templateIds.isEmpty && empty.interfaceFilters.isEmpty =>
          party
      }
      .map(_.toString -> Set.empty[Identifier])

    val templateFilters = (for {
      (party, filters) <- domainTransactionFilter.filtersByParty.iterator
      inclusiveFilters <- filters.inclusive.iterator
      templateId <- inclusiveFilters.templateIds.iterator
    } yield party.toString -> templateId)
      .toSet[(String, Identifier)]
      .groupMap(_._1)(_._2)

    // wildcard filters must take precedence over template filters
    // therefore the order of concatenation matters here.
    templateFilters ++ wildcardFilters
  }

  private def populateInterfaceView(
      domainTransactionFilter: domain.TransactionFilter,
      interfaceImplementedBy: Identifier => Set[Identifier],
  ): Map[String, Map[Identifier, Set[Identifier]]] = (for {
    (party, filters) <- domainTransactionFilter.filtersByParty.iterator
    inclusiveFilters <- filters.inclusive.iterator
    interfaceFilter <- inclusiveFilters.interfaceFilters.iterator
    if interfaceFilter.includeView
    implementor <- interfaceImplementedBy(interfaceFilter.interfaceId).iterator
  } yield (party, implementor, interfaceFilter.interfaceId))
    .toSet[(Party, Identifier, Identifier)]
    .groupMap(_._1) { case (_, templateId, interfaceId) => templateId -> interfaceId }
    .map { case (partyId, templateAndInterfacePairs) =>
      (partyId.toString, templateAndInterfacePairs.groupMap(_._1)(_._2))
    }
}
