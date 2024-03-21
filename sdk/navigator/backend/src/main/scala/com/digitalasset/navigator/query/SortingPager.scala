// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import com.daml.navigator.dotnot.{OnTreeReady, PropertyCursor}
import com.daml.navigator.model._
import com.daml.navigator.query.project._
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec

sealed abstract class SortingPager[N <: Node[_]](
    criteria: List[SortCriterion],
    project: OnTreeReady[N, ProjectValue, DamlLfTypeLookup],
    ps: DamlLfTypeLookup,
) extends PagerDecorator[N]
    with LazyLogging {

  final override def decorate(page: Page[N], ledger: Ledger): Page[N] =
    page.copy(rows = sort(page.rows), sortedLike = criteria)

  def sort(rows: Seq[N]): Seq[N] =
    rows.sortBy(projectAll)(ordering)

  private val ordering =
    new Ordering[List[Option[ProjectValue]]] {
      val optionProjectValueOrdering = Ordering[Option[ProjectValue]]

      override def compare(l1: List[Option[ProjectValue]], l2: List[Option[ProjectValue]]): Int = {
        @tailrec
        def loop(
            l1: List[Option[ProjectValue]],
            l2: List[Option[ProjectValue]],
            criteria: List[SortCriterion],
        ): Int = {
          l1 match {
            case Nil => 0
            case x1 :: xs1 =>
              val x2 :: xs2 = l2
              val c :: cs = criteria
              optionProjectValueOrdering.compare(x1, x2) match {
                case 0 => loop(xs1, xs2, cs)
                case x =>
                  c.direction match {
                    case SortDirection.DESCENDING => x * (-1)
                    case SortDirection.ASCENDING => x
                  }
              }
          }
        }
        loop(l1, l2, criteria)
      }
    }

  private def projectAll(node: N): List[Option[ProjectValue]] =
    criteria.map(project(node))

  private def project(node: N)(criterion: SortCriterion): Option[ProjectValue] = {
    val cursor = PropertyCursor.fromString(criterion.field)
    val x = project.run(node, cursor, "", ps)
    x match {
      case Left(failure) =>
        logger.error(s"Cannot project $node with criterion $criterion: $failure. Using None.")
        None
      case Right(value) =>
        Some(value)
    }
  }
}

final class ContractSorter(
    val criteria: List[SortCriterion],
    ps: DamlLfTypeLookup,
    val delegate: Pager[Contract],
) extends SortingPager[Contract](criteria, contractProject, ps)

final class TemplateSorter(
    val criteria: List[SortCriterion],
    ps: DamlLfTypeLookup,
    val delegate: Pager[Template],
) extends SortingPager[Template](criteria, templateProject, ps)

final class CommandSorter(
    val criteria: List[SortCriterion],
    ps: DamlLfTypeLookup,
    val delegate: Pager[Command],
) extends SortingPager[Command](criteria, commandProject, ps)
