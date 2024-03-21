// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import com.daml.navigator.model._

case class Page[N <: Node[_]](offset: Int, total: Int, rows: Seq[N], sortedLike: Seq[SortCriterion])

@SuppressWarnings(Array("org.wartremover.warts.Enumeration"))
object SortDirection extends Enumeration {
  val ASCENDING, DESCENDING = Value
}

case class SortCriterion(field: String, direction: SortDirection.Value)

sealed abstract class FilterCriterionBase
final case class OrFilterCriterion(filters: List[FilterCriterionBase]) extends FilterCriterionBase
final case class AndFilterCriterion(filters: List[FilterCriterionBase]) extends FilterCriterionBase
final case class FilterCriterion(field: String, value: String) extends FilterCriterionBase

trait Pager[N <: Node[_]] {
  def fetch(ledger: Ledger, templates: PackageRegistry): Page[N]
}

final object ActiveContractsPager extends Pager[Contract] {
  override def fetch(ledger: Ledger, templates: PackageRegistry): Page[Contract] =
    Page(0, ledger.activeContractsCount, ledger.activeContracts(templates), Seq.empty)
}

final object AllContractsPager extends Pager[Contract] {
  override def fetch(ledger: Ledger, templates: PackageRegistry): Page[Contract] =
    Page(0, ledger.allContractsCount, ledger.allContracts(templates), Seq.empty)
}

final object TemplatePager extends Pager[Template] {
  override def fetch(ledger: Ledger, templates: PackageRegistry): Page[Template] =
    Page(0, templates.templateCount, templates.allTemplates(), Seq.empty)
}

final class TemplateContractPager(template: Template) extends Pager[Contract] {
  override def fetch(ledger: Ledger, templates: PackageRegistry): Page[Contract] = {
    val contracts = ledger.templateContractsOf(template, templates)
    Page(0, contracts.size, contracts, Seq.empty)
  }
}

final class ActiveTemplateContractPager(template: Template) extends Pager[Contract] {
  override def fetch(ledger: Ledger, templates: PackageRegistry): Page[Contract] = {
    val contracts = ledger.activeTemplateContractsOf(template, templates)
    Page(0, contracts.size, contracts, Seq.empty)
  }
}

final object CommandPager extends Pager[Command] {
  override def fetch(ledger: Ledger, templates: PackageRegistry): Page[Command] = {
    val commands = ledger.allCommands(templates)
    Page(0, commands.size, commands, Seq.empty)
  }
}

trait PagerDecorator[N <: Node[_]] extends Pager[N] {
  override def fetch(ledger: Ledger, templates: PackageRegistry): Page[N] =
    decorate(delegate.fetch(ledger, templates), ledger)

  def delegate: Pager[N]

  def decorate(page: Page[N], ledger: Ledger): Page[N]
}

class BoundingPager[N <: Node[_]](size: Int, val delegate: Pager[N]) extends PagerDecorator[N] {
  override def decorate(page: Page[N], ledger: Ledger): Page[N] =
    page.copy(rows = page.rows.take(size))
}

class ShiftingPager[N <: Node[_]](previous: String, val delegate: Pager[N])
    extends PagerDecorator[N] {
  override def decorate(page: Page[N], ledger: Ledger): Page[N] = {
    val shiftedRows = page.rows.dropWhile(_.id != previous).drop(1)
    page.copy(offset = page.rows.size - shiftedRows.size, rows = shiftedRows)
  }
}
