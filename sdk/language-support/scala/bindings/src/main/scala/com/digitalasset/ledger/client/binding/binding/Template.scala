// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    Template.CreateForExercise(self)

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
      private[binding] val value: Template[_]
  ) {

    /** Get access to interface choices.
      *
      * {{{
      *  MyTemplate(foo, bar).createAnd
      *    .toInterface[MyInterface]
      *    .exerciseInterfaceChoice(controller, ...)
      * }}}
      */
    @nowarn("cat=unused&msg=parameter ev in method")
    def toInterface[I](implicit ev: ToInterface[T, I]): CreateForExercise[I] =
      CreateForExercise(value)
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
  )

  /** Evidence that coercing from template-IDed to interface-IDed is sound,
    * i.e. `toInterface`.  Not safe at all for the opposite coercion.
    *
    * This weaker marker is defined so that [[Key]] and [[CreateForExercise]] can
    * have simple `toInterface` methods.
    */
  @implicitNotFound("${T} is not a template that implements interface ${I}")
  sealed abstract class ToInterface[-T, I]

  /** As with [[ToInterface]], but also proves that `T` implements `I`.
    * This is a subtle distinction, but [[ToInterface]] allows a universe of
    * subtypes for which this would not be true.  So this can be used for
    * `unsafeToTemplate` as well as `toInterface`.
    */
  @implicitNotFound("${T} is not a template that implements interface ${I}")
  final class Implements[T, I] extends ToInterface[T, I]

  import Primitive.ContractId, ContractId.subst
  implicit final class `template ContractId syntax`[T](private val self: ContractId[T])
      extends AnyVal {

    /** Widen a contract ID to the same contract ID for one of `T`'s implemented
      * interfaces.  Do this to get access to the interface choices.
      */
    @nowarn("cat=unused&msg=parameter ev in method")
    def toInterface[I](implicit ev: ToInterface[T, I]): ContractId[I] = {
      type K[C] = C => ApiTypes.ContractId
      type K2[C] = ContractId[T] => C
      subst[K2, I](subst[K, T](identity))(self)
    }
  }
}
