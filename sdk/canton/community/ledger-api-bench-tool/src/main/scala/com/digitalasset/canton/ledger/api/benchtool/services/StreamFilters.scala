// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  InterfaceFilter,
  TemplateFilter,
  WildcardFilter,
}
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig

object StreamFilters {

  def eventFormat(
      filters: List[WorkflowConfig.StreamConfig.PartyFilter]
  ): Either[String, EventFormat] =
    toEitherList(filters.map(toTransactionFilter))
      .map { byPartyFilters =>
        EventFormat.defaultInstance.withFiltersByParty(byPartyFilters.toMap)
      }

  private def toTransactionFilter(
      filter: WorkflowConfig.StreamConfig.PartyFilter
  ): Either[String, (String, Filters)] =
    ((filter.templates, filter.interfaces) match {
      case (Nil, Nil) =>
        Right(Filters.defaultInstance)
      case (templateIds, interfaceIds) =>
        for {
          tplIds <- templateIdentifiers(templateIds)
          ifaceIds <- templateIdentifiers(interfaceIds)
        } yield {
          val interfaceFilters =
            ifaceIds.map(interfaceId =>
              InterfaceFilter(
                interfaceId = Some(interfaceId),
                includeInterfaceView = true,
                includeCreatedEventBlob = false,
              )
            )
          val templateFilters =
            tplIds.map(templateId =>
              TemplateFilter(
                templateId = Some(templateId),
                includeCreatedEventBlob = false,
              )
            )
          val templateWildcardFilterO =
            Option.when(tplIds.isEmpty && ifaceIds.isEmpty)(
              WildcardFilter(
                includeCreatedEventBlob = false
              )
            )

          Filters.defaultInstance.withCumulative(
            interfaceFilters.map(CumulativeFilter.defaultInstance.withInterfaceFilter) ++
              templateFilters.map(CumulativeFilter.defaultInstance.withTemplateFilter) ++
              (templateWildcardFilterO match {
                case Some(templateWildcardFilter) =>
                  Seq(CumulativeFilter.defaultInstance.withWildcardFilter(templateWildcardFilter))
                case None => Seq.empty
              })
          )
        }
    }).map(templateFilters => filter.party -> templateFilters)

  private def templateIdentifiers(templates: List[String]): Either[String, List[Identifier]] =
    toEitherList(templates.map(templateIdFromString))

  private def templateIdFromString(fullyQualifiedTemplateId: String): Either[String, Identifier] =
    fullyQualifiedTemplateId
      .split(':')
      .toList match {
      case packageId :: moduleName :: entityName :: Nil =>
        Right(
          Identifier.defaultInstance
            .withEntityName(entityName)
            .withModuleName(moduleName)
            .withPackageId(packageId)
        )
      case _ =>
        Left(s"Invalid template id: $fullyQualifiedTemplateId")
    }

  private def toEitherList[L, R](l: List[Either[L, R]]): Either[L, List[R]] =
    l.foldLeft[Either[L, List[R]]](Right(List.empty[R])) { case (acc, next) =>
      for {
        elems <- acc
        elem <- next
      } yield elem :: elems
    }

}
