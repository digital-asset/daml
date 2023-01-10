// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.refinements.ApiTypes.Choice
import com.daml.ledger.api.v1.{event => rpcevent, value => rpcvalue}
import rpcvalue.Value.{Sum => VSum}
import encoding.{LfEncodable, LfTypeEncoding, RecordView}

/** Common superclass of template classes' companions.
  *
  * @tparam T The companion class's type. We can get away with this here, but
  *           not for [[ValueRefCompanion]], because templates' associated
  *           types are guaranteed to have zero tparams.
  */
abstract class TemplateCompanion[T](implicit isTemplate: T <:< Template[T])
    extends ContractTypeCompanion[T]
    with LfEncodable.ViaFields[T] {

  val consumingChoices: Set[Choice]

  /** Permits TemplateCompanion to be optionally treated as a typeclass for
    * template types.
    */
  implicit final def `the TemplateCompanion`: this.type = this

  /** The template's key type, or [[Nothing]] if there is no key type. */
  type key

  /** Prepare an exercise-by-key Update. */
  final def key(k: key)(implicit enc: ValueEncoder[key]): Template.Key[T] =
    Template.Key(Value encode k, this)

  /** Proof that T <: Template[T].  Expressed here instead of as a type parameter
    * bound because the latter is much more inconvenient in practice.
    */
  val describesTemplate: T <:< Template[T] = isTemplate

  def toNamedArguments(associatedType: T): rpcvalue.Record

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

  /** Helper for EventDecoderApi. */
  private[binding] final def decoderEntry
      : (Primitive.TemplateId[T], rpcevent.CreatedEvent => Option[Template[T]]) = {
    type K[+A] = (Primitive.TemplateId[T], rpcevent.CreatedEvent => Option[A])
    describesTemplate.substituteCo[K](
      (id, _.createArguments flatMap fromNamedArguments)
    )
  }

  protected final def ` arguments`(elems: (String, rpcvalue.Value)*): rpcvalue.Record =
    Primitive.arguments(` dataTypeId`, elems)
}

object TemplateCompanion {
  abstract class Empty[T](implicit isTemplate: T <:< Template[T]) extends TemplateCompanion[T] {
    protected def onlyInstance: T
    override type key = Nothing
    type view[C[_]] = RecordView.Empty[C]
    override def toNamedArguments(associatedType: T) = ` arguments`()
    override def fromNamedArguments(namedArguments: rpcvalue.Record): Option[T] = Some(onlyInstance)
    override def fieldEncoding(lte: LfTypeEncoding): view[lte.Field] = RecordView.Empty
    override def encoding(lte: LfTypeEncoding)(view: view[lte.Field]): lte.Out[T] =
      lte.emptyRecord(` dataTypeId`, () => onlyInstance)
  }
}
