// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.lf.data.Ref.*
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{Filters, InclusiveFilters}
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection

import scala.collection.View

/**  This class encapsulates the logic of how contract arguments and interface views are
  *  being projected to the consumer based on the filter criteria and the relation between
  *  interfaces and templates implementing them.
  *
  * @param verbose enriching in verbose mode
  * @param wildcardWitnesses all the wildcard parties for which contract arguments will be populated
  * @param witnessTemplateProjections per witness party, per template projections
  */
final case class EventProjectionProperties(
    verbose: Boolean,
    wildcardWitnesses: Set[String],
    // Map(witness -> Map(template -> projection))
    witnessTemplateProjections: Map[String, Map[Identifier, Projection]] = Map.empty,
    alwaysPopulateCreatedEventBlob: Boolean = false,
) {
  def render(witnesses: Set[String], templateId: Identifier): Projection =
    witnesses.iterator
      .flatMap(witnessTemplateProjections.get(_).iterator)
      .flatMap(_.get(templateId).iterator)
      .foldLeft(
        Projection(
          contractArguments = witnesses.exists(wildcardWitnesses),
          createdEventBlob = alwaysPopulateCreatedEventBlob,
        )
      )(_ append _)
}

object EventProjectionProperties {

  final case class Projection(
      interfaces: Set[Identifier] = Set.empty,
      createdEventBlob: Boolean = false,
      contractArguments: Boolean = false,
  ) {
    def append(other: Projection): Projection =
      Projection(
        interfaces ++ other.interfaces,
        createdEventBlob || other.createdEventBlob,
        contractArguments || other.contractArguments,
      )
  }

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
      resolveTemplateIds: TypeConRef => Set[Identifier],
      alwaysPopulateArguments: Boolean,
  ): EventProjectionProperties =
    EventProjectionProperties(
      verbose = verbose,
      wildcardWitnesses = wildcardWitnesses(transactionFilter, alwaysPopulateArguments),
      witnessTemplateProjections =
        witnessTemplateProjections(transactionFilter, interfaceImplementedBy, resolveTemplateIds),
      alwaysPopulateCreatedEventBlob = transactionFilter.alwaysPopulateCreatedEventBlob,
    )

  private def wildcardWitnesses(
      domainTransactionFilter: domain.TransactionFilter,
      alwaysPopulateArguments: Boolean,
  ): Set[String] =
    if (alwaysPopulateArguments)
      domainTransactionFilter.filtersByParty.keysIterator
        .map(_.toString)
        .toSet
    else
      domainTransactionFilter.filtersByParty.iterator
        .collect {
          case (party, Filters(None)) => party
          case (party, Filters(Some(empty)))
              if empty.templateFilters.isEmpty && empty.interfaceFilters.isEmpty =>
            party
        }
        .map(_.toString)
        .toSet

  private def witnessTemplateProjections(
      domainTransactionFilter: domain.TransactionFilter,
      interfaceImplementedBy: Identifier => Set[Identifier],
      resolveTemplateIds: TypeConRef => Set[Identifier],
  ): Map[String, Map[Identifier, Projection]] =
    (for {
      (party, filters) <- domainTransactionFilter.filtersByParty.view
      inclusiveFilters <- filters.inclusive.toList.view
    } yield {
      val interfaceFilterProjections = for {
        interfaceFilter <- inclusiveFilters.interfaceFilters.view
        implementor <- interfaceImplementedBy(interfaceFilter.interfaceId).view
      } yield implementor -> Projection(
        interfaces =
          if (interfaceFilter.includeView) Set(interfaceFilter.interfaceId) else Set.empty,
        createdEventBlob = interfaceFilter.includeCreatedEventBlob,
        contractArguments = false,
      )
      val templateProjections = getTemplateProjections(inclusiveFilters, resolveTemplateIds)
      val projectionsForParty =
        (interfaceFilterProjections ++ templateProjections)
          .groupMap(_._1)(_._2)
          .view
          .mapValues(_.foldLeft(Projection())(_ append _))
          .toMap

      party -> projectionsForParty
    }).toMap

  private def getTemplateProjections(
      inclusiveFilters: InclusiveFilters,
      resolveTemplateIds: TypeConRef => Set[Identifier],
  ): View[(Identifier, Projection)] =
    for {
      templateFilter <- inclusiveFilters.templateFilters.view
      templateId <- resolveTemplateIds(templateFilter.templateTypeRef).view
    } yield templateId -> Projection(
      interfaces = Set.empty,
      createdEventBlob = templateFilter.includeCreatedEventBlob,
      contractArguments = true,
    )
}
