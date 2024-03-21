// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import com.daml.navigator.dotnot.{OnTreeReady, PropertyCursor}
import com.daml.navigator.model._
import com.daml.navigator.query.filter._
import com.typesafe.scalalogging.LazyLogging

sealed abstract class FilteringPager[N <: Node[_]](
    criterion: FilterCriterionBase,
    filter: OnTreeReady[N, Boolean, DamlLfTypeLookup],
    ps: DamlLfTypeLookup,
) extends PagerDecorator[N]
    with LazyLogging {

  final override def decorate(page: Page[N], ledger: Ledger): Page[N] = {
    val included = page.rows.filter(isIncluded)
    page.copy(total = included.size, rows = included)
  }

  private[query] def isIncluded(node: N): Boolean = {
    def loop(criterion: FilterCriterionBase): Boolean = {
      criterion match {
        case AndFilterCriterion(criteria) =>
          criteria.forall(loop)
        case OrFilterCriterion(criteria) =>
          criteria.exists(loop)
        case criterion: FilterCriterion =>
          matchCriterion(node)(criterion)
      }
    }
    loop(criterion)
  }

  private[query] def matchCriterion(node: N)(criterion: FilterCriterion): Boolean = {
    val cursor = PropertyCursor.fromString(criterion.field)
    filter.run(node, cursor, criterion.value, ps) match {
      case Left(failure) =>
        logger.error(
          s"Cannot match $node and " +
            s"criterion $criterion: $failure. Excluding it."
        )
        false
      case Right(isMatching) =>
        isMatching
    }
  }
}

final class ContractFilter(
    criterion: FilterCriterionBase,
    ps: DamlLfTypeLookup,
    val delegate: Pager[Contract],
) extends FilteringPager[Contract](criterion, contractFilter, ps)

final class TemplateFilter(
    criterion: FilterCriterionBase,
    ps: DamlLfTypeLookup,
    val delegate: Pager[Template],
) extends FilteringPager[Template](criterion, templateFilter, ps)

final class CommandFilter(
    criterion: FilterCriterionBase,
    ps: DamlLfTypeLookup,
    val delegate: Pager[Command],
) extends FilteringPager[Command](criterion, commandFilter, ps)
