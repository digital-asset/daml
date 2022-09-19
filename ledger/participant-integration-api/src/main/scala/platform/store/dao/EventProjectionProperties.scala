// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{Filters, InclusiveFilters}
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.platform.store.dao.EventProjectionProperties.RenderResult

/**  This class encapsulates the logic of how contract arguments and interface views are
  *  being projected to the consumer based on the filter criteria and the relation between
  *  interfaces and templates implementing them.
  *
  * @param verbose enriching in verbose mode
  * @param witnessTemplateIdFilter populate contract_argument, and contract_key. If templateId set is empty: populate.
  * @param witnessInterfaceViewFilter populate interface_views. The Map of templates to interfaces,
  *                              and the set of implementor templates cannot be empty.
  * @param witnessContractArgumentBlob populate contract_arguments_blob.
  */
final case class EventProjectionProperties private[dao] (
    verbose: Boolean,
    // Map(eventWitnessParty, Set(templateId))
    witnessTemplateIdFilter: Map[String, Set[Identifier]] = Map.empty,
    // Map(eventWitnessParty, Map(templateId -> Set(interfaceId)))
    witnessInterfaceViewFilter: Map[String, Map[Identifier, Set[Identifier]]] = Map.empty,
    // Set of parties which require to populate contractArgumentBlob
    witnessContractArgumentBlob: Set[String] = Set.empty,
) {
  def render(witnesses: Set[String], templateId: Identifier): RenderResult = {
    val renderContractArguments: Boolean = witnesses.view
      .flatMap(witnessTemplateIdFilter.get)
      .exists(templates => templates.isEmpty || templates(templateId))

    val interfacesToRender: Set[Identifier] = witnesses.view
      .flatMap(witnessInterfaceViewFilter.get(_).iterator)
      .flatMap(_.getOrElse(templateId, Set.empty[Identifier]))
      .toSet

    val contractArgumentsBlob =
      witnesses.intersect(witnessContractArgumentBlob).nonEmpty

    RenderResult(
      contractArgumentsBlob,
      renderContractArguments,
      interfacesToRender,
    )
  }

}

object EventProjectionProperties {

  case class RenderResult(
      contractArgumentsBlob: Boolean,
      contractArguments: Boolean,
      interfaces: Set[Identifier],
  )

  /** @param transactionFilter     Transaction filter as defined by the consumer of the API.
    * @param verbose                enriching in verbose mode
    * @param interfaceImplementedBy The relation between an interface id and template id.
    *                               If template has no relation to the interface,
    *                               an empty Set must be returned.
    */
  def apply(
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
      interfaceImplementedBy: Identifier => Set[Identifier],
  ): EventProjectionProperties =
    EventProjectionProperties(
      verbose = verbose,
      witnessTemplateIdFilter = witnessTemplateIdFilter(transactionFilter),
      witnessInterfaceViewFilter =
        witnessInterfaceViewFilter(transactionFilter, interfaceImplementedBy),
      witnessContractArgumentBlob = transactionFilter.filtersByParty.iterator.collect {
        case (party, Filters(Some(InclusiveFilters(_, _, true)))) => party.toString
      }.toSet,
    )

  private def witnessTemplateIdFilter(
      domainTransactionFilter: domain.TransactionFilter
  ): Map[String, Set[Identifier]] =
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

  private def witnessInterfaceViewFilter(
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
