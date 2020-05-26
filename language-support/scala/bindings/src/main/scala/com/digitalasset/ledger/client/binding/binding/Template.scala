// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.refinements.ApiTypes.Choice
import com.daml.ledger.api.v1.{value => rpcvalue}

abstract class Template[+T] extends ValueRef { self: T =>
  final def create(implicit d: DummyImplicit): Primitive.Update[Primitive.ContractId[T]] =
    Primitive.createFromArgs(templateCompanion, templateCompanion.toNamedArguments(self))

  /** Part of a `CreateAndExercise` command.
    *
    * {{{
    *   Iou(foo, bar).createAnd.exerciseTransfer(controller, ...)
    * }}}
    */
  final def createAnd(implicit d: DummyImplicit): Template.CreateForExercise[T] =
    Template.CreateForExercise(self)

  final def arguments(implicit d: DummyImplicit): rpcvalue.Record =
    templateCompanion.toNamedArguments(self)

  final def templateId(implicit d: DummyImplicit): Primitive.TemplateId[T] =
    templateCompanion.id

  final def consumingChoices(implicit d: DummyImplicit): Set[Choice] =
    templateCompanion.consumingChoices

  // arguments and templateId are provided in lieu of making templateCompanion
  // public, though the latter might be more "powerful"
  protected[this] def templateCompanion(
      implicit d: DummyImplicit): TemplateCompanion[_ >: self.type <: T]
}

object Template {

  /** Part of a `CreateAndExercise` command.
    *
    * {{{
    *   Iou(foo, bar).createAnd.exerciseTransfer(controller, ...)
    * }}}
    */
  final case class CreateForExercise[+T](value: T with Template[T])
}
