// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import scala.language.higherKinds

import com.daml.ledger.api.refinements.ApiTypes.{Choice, TemplateId}
import com.daml.ledger.api.v1.{event => rpcevent, value => rpcvalue}
import rpcvalue.Value.{Sum => VSum}
import encoding.{ExerciseOn, LfEncodable, LfTypeEncoding, RecordView}

import scalaz.Liskov
import Liskov.<~<

/** Common superclass of template classes' companions.
  *
  * @tparam T The companion class's type. We can get away with this here, but
  *           not for [[ValueRefCompanion]], because templates' associated
  *           types are guaranteed to have zero tparams.
  */
abstract class TemplateCompanion[T](implicit isTemplate: T <~< Template[T])
    extends ValueRefCompanion
    with LfEncodable.ViaFields[T] {

  /** Alias for contract IDs for this template. Can be used interchangeably with
    * its expansion.
    */
  type ContractId = Primitive.ContractId[T]

  val id: Primitive.TemplateId[T]

  val consumingChoices: Set[Choice]

  /** Permits TemplateCompanion to be optionally treated as a typeclass for
    * template types.
    */
  implicit final def `the TemplateCompanion`: this.type = this

  /** Proof that T <: Template[T].  Expressed here instead of as a type parameter
    * bound because the latter is much more inconvenient in practice.
    */
  val describesTemplate: T <~< Template[T] = isTemplate

  def toNamedArguments(associatedType: T): rpcvalue.Record

  override protected lazy val ` dataTypeId` = TemplateId.unwrap(id)

  def fromNamedArguments(namedArguments: rpcvalue.Record): Option[T]

  implicit val `the template Value`: Value[T] = new `Value ValueRef`[T] {
    override def read(argumentValue: VSum): Option[T] =
      argumentValue.record flatMap fromNamedArguments
    override def write(obj: T): VSum = VSum.Record(toNamedArguments(obj))
  }

  implicit val `the template LfEncodable`: LfEncodable[T] = new LfEncodable[T] {
    override def encoding(lte: LfTypeEncoding): lte.Out[T] =
      TemplateCompanion.this.encoding(lte)(TemplateCompanion.this.fieldEncoding(lte))
  }

  protected final def ` templateId`(
      packageId: String,
      moduleName: String,
      entityName: String): Primitive.TemplateId[T] =
    Primitive.TemplateId(packageId, moduleName, entityName)

  /** Helper for EventDecoderApi. */
  private[binding] final def decoderEntry
    : (Primitive.TemplateId[T], rpcevent.CreatedEvent => Option[Template[T]]) = {
    type K[+A] = (Primitive.TemplateId[T], rpcevent.CreatedEvent => Option[A])
    Liskov.co[K, T, Template[T]](describesTemplate)(
      (id, _.createArguments flatMap fromNamedArguments))
  }

  protected final def ` exercise`[ExOn, Out](
      receiver: ExOn,
      choiceId: String,
      arguments: Option[rpcvalue.Value])(
      implicit exon: ExerciseOn[ExOn, T]): Primitive.Update[Out] =
    Primitive.exercise(this, receiver, choiceId, arguments getOrElse Value.encode(()))

  protected final def ` arguments`(elems: (String, rpcvalue.Value)*): rpcvalue.Record =
    Primitive.arguments(` dataTypeId`, elems)
}

object TemplateCompanion {
  abstract class Empty[T](implicit isTemplate: T <~< Template[T]) extends TemplateCompanion[T] {
    protected def onlyInstance: T
    type view[C[_]] = RecordView.Empty[C]
    override def toNamedArguments(associatedType: T) = ` arguments`()
    override def fromNamedArguments(namedArguments: rpcvalue.Record): Option[T] = Some(onlyInstance)
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override def fieldEncoding(lte: LfTypeEncoding): view[lte.Field] = RecordView.Empty
    override def encoding(lte: LfTypeEncoding)(view: view[lte.Field]): lte.Out[T] =
      lte.emptyRecord(` dataTypeId`, () => onlyInstance)
  }
}
