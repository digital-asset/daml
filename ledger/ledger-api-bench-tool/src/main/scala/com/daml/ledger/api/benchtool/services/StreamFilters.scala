// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.value.Identifier

object StreamFilters {

  def transactionFilters(
      filters: List[WorkflowConfig.StreamConfig.PartyFilter]
  ): Either[String, TransactionFilter] =
    toEitherList(filters.map(toTransactionFilter))
      .map { byPartyFilters =>
        TransactionFilter.defaultInstance.withFiltersByParty(byPartyFilters.toMap)
      }

  private def toTransactionFilter(
      filter: WorkflowConfig.StreamConfig.PartyFilter
  ): Either[String, (String, Filters)] =
    (filter.templates match {
      case Nil =>
        Right(Filters.defaultInstance)
      case templateIds =>
        templateIdentifiers(templateIds).map { identifiers =>
          Filters.defaultInstance.withInclusive(
            InclusiveFilters.defaultInstance.addAllTemplateIds(identifiers)
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
