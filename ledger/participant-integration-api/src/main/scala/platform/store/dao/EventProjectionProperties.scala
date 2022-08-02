// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.Filters
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Party}

/** @param verbose enriching in verbose mode
  * @param populateContractArgument populate contract_argument, and contract_key. If templateId set is empty: populate.
  * @param populateInterfaceView populate interface_views. The Map of templates to interfaces,
  *                              and the set of implementor templates cannot be empty.
  */
// TODO DPP-1068: [implementation detail] unit testing
case class EventProjectionProperties(
    verbose: Boolean,
    // Map(eventWitnessParty, Set(templateId))
    populateContractArgument: Map[String, Set[Identifier]] = Map.empty,
    // Map(eventWitnessParty, Map(templateId -> Set(interfaceId)))
    populateInterfaceView: Map[String, Map[Identifier, Set[Identifier]]] = Map.empty,
) {
  def renderContractArguments(witnesses: Seq[String], templateId: Identifier): Boolean =
    witnesses
      .map(populateContractArgument.get)
      .exists {
        case Some(wildcardTemplates) if wildcardTemplates.isEmpty => true
        case Some(nonEmptyTemplates) if nonEmptyTemplates(templateId) => true
        case _ => false
      }

  def renderInterfaces(witnesses: Seq[String], templateId: Identifier): Iterator[Identifier] =
    for {
      witness <- witnesses.iterator
      templateToInterfaceMap <- populateInterfaceView.get(witness).toList
      interfaces <- templateToInterfaceMap.getOrElse(templateId, Set.empty[Identifier])
    } yield interfaces
}

object EventProjectionProperties {

  def apply(
      domainTransactionFilter: domain.TransactionFilter,
      verbose: Boolean,
      interfaceImplementedBy: Ref.Identifier => Option[Set[Ref.Identifier]],
  ): EventProjectionProperties = {
    EventProjectionProperties(
      verbose = verbose,
      populateContractArgument = populateContractArgument(domainTransactionFilter),
      populateInterfaceView = populateInterfaceView(domainTransactionFilter, interfaceImplementedBy),
    )
  }

  private def populateContractArgument(domainTransactionFilter: domain.TransactionFilter) = {
    (for {
      (party, filters) <- domainTransactionFilter.filtersByParty.iterator
      inclusiveFilters <- filters.inclusive.iterator
      templateId <- inclusiveFilters.templateIds.iterator
    } yield party.toString -> templateId)
      .toSet[(String, Identifier)]
      .groupMap(_._1)(_._2)
      .++(
        domainTransactionFilter.filtersByParty.iterator
          .collect {
            case (party, Filters(None)) =>
              party.toString -> Set.empty[Identifier]

            case (party, Filters(Some(empty)))
                if empty.templateIds.isEmpty && empty.interfaceFilters.isEmpty =>
              party.toString -> Set.empty[Identifier]
          }
      )
  }

  private def populateInterfaceView(
      domainTransactionFilter: domain.TransactionFilter,
      interfaceImplementedBy: Identifier => Option[Set[Ref.Identifier]],
  ) = (for {
    (party, filters) <- domainTransactionFilter.filtersByParty.iterator
    inclusiveFilters <- filters.inclusive.iterator
    interfaceFilter <- inclusiveFilters.interfaceFilters.iterator
    if interfaceFilter.includeView
    implementors <- interfaceImplementedBy(interfaceFilter.interfaceId).iterator
    implementor <- implementors
  } yield (party, implementor, interfaceFilter.interfaceId))
    .toSet[(Party, Identifier, Identifier)]
    .groupMap(_._1) { case (_, templateId, interfaceId) => templateId -> interfaceId }
    .map { case (key, value) =>
      (key.toString, value.groupMap(_._1)(_._2))
    }
}
