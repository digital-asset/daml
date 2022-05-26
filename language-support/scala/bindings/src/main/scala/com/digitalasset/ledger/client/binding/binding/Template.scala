// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.refinements.ApiTypes
import ApiTypes.Choice
import com.daml.ledger.api.v1.{value => rpcvalue}

import scala.annotation.{nowarn, implicitNotFound}

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
    Template.CreateForExercise(self, templateCompanion)

  final def arguments(implicit d: DummyImplicit): rpcvalue.Record =
    templateCompanion.toNamedArguments(self)

  final def templateId(implicit d: DummyImplicit): Primitive.TemplateId[T] =
    templateCompanion.id

  final def consumingChoices(implicit d: DummyImplicit): Set[Choice] =
    templateCompanion.consumingChoices

  // arguments and templateId are provided in lieu of making templateCompanion
  // public, though the latter might be more "powerful"
  protected[this] def templateCompanion(implicit
      d: DummyImplicit
  ): TemplateCompanion[_ >: self.type <: T]
}

object Template {

  /** Part of a `CreateAndExercise` command.
    *
    * {{{
    *   Iou(foo, bar).createAnd.exerciseTransfer(controller, ...)
    * }}}
    */
  final case class CreateForExercise[+T](
      private[binding] val value: Template[_],
      private[binding] val origin: TemplateCompanion[_],
  ) {
    @nowarn("cat=unused&msg=parameter value ev in method")
    def toInterface[T0 >: T, I](implicit ev: Implements[T0, I]): CreateForExercise[I] =
      CreateForExercise(value, origin)
  }

  /** Part of an `ExerciseByKey` command.
    *
    * {{{
    *   Iou.key(foo).exerciseTransfer(controller, ...)
    * }}}
    */
  final case class Key[+T](
      private[binding] val encodedKey: rpcvalue.Value,
      private[binding] val origin: TemplateCompanion[_],
  ) {
    @nowarn("cat=unused&msg=parameter value ev in method")
    def toInterface[T0 >: T, I](implicit ev: Implements[T0, I]): Key[I] =
      Key(encodedKey, origin)
  }

  @implicitNotFound("${T} is not a template that implements interface ${I}")
  final class Implements[T, I]

  import Primitive.ContractId, ContractId.subst
  implicit final class `template ContractId syntax`[T](private val self: ContractId[T])
      extends AnyVal {
    @nowarn("cat=unused&msg=parameter value ev in method")
    def toInterface[I](implicit ev: Implements[T, I]): ContractId[I] = {
      type K[C] = C => ApiTypes.ContractId
      type K2[C] = ContractId[T] => C
      subst[K2, I](subst[K, T](identity))(self)
    }
  }
}
