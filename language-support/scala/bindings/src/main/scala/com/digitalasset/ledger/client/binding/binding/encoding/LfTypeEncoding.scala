// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding
package encoding

import scala.language.higherKinds

import scalaz.{OneAnd, Plus}
import scalaz.syntax.foldable1._
import com.digitalasset.ledger.api.v1.{value => rpcvalue}

/** A backend for accumulating well-typed information about DAML-LF ADTs
  * (records, variants, and templates) into encodings of those ADTs.
  *
  * While a few of its methods are concrete, implementers of
  * [[LfTypeEncoding]] can assume that those implementations are fixed,
  * and should be expressed solely in terms of the abstract methods and
  * types.
  */
trait LfTypeEncoding {
  type Out[A]

  /** A guaranteed-single field encoding. */
  type Field[A]

  /** An encoding for one or more fields, with associated operations for
    * combination.
    */
  type RecordFields[A]

  type VariantCases[A]

  type EnumCases[A]

  /** "Finalize" a field set. */
  def record[A](recordId: rpcvalue.Identifier, fi: RecordFields[A]): Out[A]

  /** `record` produces a non-empty record; alternatively, produce an empty
    * record with this function.  Not incorporating an identity in `RecordFields`
    * to solve this problem significantly simplifies the design of useful
    * `RecordFields` types.
    *
    * @param element A simplification of the arguments to `xmap` for
    *                `RecordFields[Unit]`, which are `Unit => A` and `A => Unit`;
    *                the latter is always `_ => ()`.
    */
  def emptyRecord[A](recordId: rpcvalue.Identifier, element: () => A): Out[A]

  /** Turn a whole value encoding into a single field. */
  def field[A](fieldName: String, o: Out[A]): Field[A]

  /** Pull a single field into the language of field lists. */
  def fields[A](fi: Field[A]): RecordFields[A]

  /** "Finalize" a enum set. */
  def enum[A](enumId: rpcvalue.Identifier, cases: EnumCases[A]): Out[A]

  /** Convenient wrapper for `enum` and iterated `VariantCase.plus`. */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  final def enumAll[A](
      enumId: rpcvalue.Identifier,
      case0: EnumCases[A],
      cases: EnumCases[A]*): Out[A] = {
    import scalaz.std.iterable._
    enum(enumId, OneAnd(case0, cases).sumr1(EnumCases.semigroup))
  }

  /** Pull a enum case into the language of enum cases.
    */
  def enumCase[A](caseName: String)(a: A): EnumCases[A]

  /** "Finalize" a variant set. */
  def variant[A](variantId: rpcvalue.Identifier, cases: VariantCases[A]): Out[A]

  /** Convenient wrapper for `variant` and iterated `VariantCase.plus`. */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  final def variantAll[A](
      variantId: rpcvalue.Identifier,
      case0: VariantCases[A],
      cases: VariantCases[A]*): Out[A] = {
    import scalaz.std.iterable._
    variant(variantId, OneAnd(case0, cases).sumr1(VariantCases.semigroup))
  }

  /** Pull a variant case into the language of variant cases.
    *
    * @param select Must form a prism with `inject`.
    */
  def variantCase[B, A](caseName: String, o: Out[B])(inject: B => A)(
      select: A PartialFunction B): VariantCases[A]

  /** Convenience wrapper of `variantCase` for a case whose contents are a
    * splatted record.  In Scala's typical ADT encoding, variant cases
    * are subtypes of the whole variant type; we exploit that.
    */
  final def variantRecordCase[B, A >: B](
      caseName: String,
      parentVariantId: rpcvalue.Identifier,
      o: RecordFields[B])(select: A PartialFunction B): VariantCases[A] =
    variantCase(caseName, record(Value.splattedVariantId(parentVariantId, caseName), o))(
      implicitly[B <:< A])(select)

  /** A language for building up record field lists and their associated
    * [[Out]]s.  The laws need only hold up to observation by `record`.
    */
  val RecordFields: InvariantApply[RecordFields]

  /** A language for building up variant case handlers. */
  val VariantCases: Plus[VariantCases]

  /** A language for building up variant case handlers. */
  val EnumCases: Plus[EnumCases]

  /** Base axioms for primitive LF types. */
  val primitive: ValuePrimitiveEncoding[Out]
}

object LfTypeEncoding {
  type Aux[Out0[_]] = LfTypeEncoding { type Out[A] = Out0[A] }
  type Lt[+Out0[_]] = LfTypeEncoding { type Out[A] <: Out0[A] }

  type Product[+Fst[_], +Snd[_], A] = (Fst[A], Snd[A])

  def product(fst: LfTypeEncoding, snd: LfTypeEncoding): LfTypeEncoding {
    type Out[A] = Product[fst.Out, snd.Out, A]
    type Field[A] = Product[fst.Field, snd.Field, A]
    type RecordFields[A] = Product[fst.RecordFields, snd.RecordFields, A]
    type VariantCases[A] = Product[fst.VariantCases, snd.VariantCases, A]
  } = new LfTypeEncoding {
    type Out[A] = Product[fst.Out, snd.Out, A]
    type Field[A] = Product[fst.Field, snd.Field, A]
    type RecordFields[A] = Product[fst.RecordFields, snd.RecordFields, A]
    type VariantCases[A] = Product[fst.VariantCases, snd.VariantCases, A]
    type EnumCases[A] = Product[fst.EnumCases, snd.EnumCases, A]

    override def record[A](recordId: rpcvalue.Identifier, fi: RecordFields[A]): Out[A] =
      (fst.record(recordId, fi._1), snd.record(recordId, fi._2))

    override def emptyRecord[A](recordId: rpcvalue.Identifier, element: () => A): Out[A] =
      (fst.emptyRecord(recordId, element), snd.emptyRecord(recordId, element))

    override def field[A](fieldName: String, o: Out[A]): Field[A] =
      (fst.field(fieldName, o._1), snd.field(fieldName, o._2))

    override def fields[A](fi: Field[A]): RecordFields[A] =
      (fst.fields(fi._1), snd.fields(fi._2))

    override def enum[A](enumId: rpcvalue.Identifier, cases: EnumCases[A]): Out[A] =
      (fst.enum(enumId, cases._1), snd.enum(enumId, cases._2))

    override def enumCase[A](caseName: String)(a: A): EnumCases[A] =
      (fst.enumCase(caseName)(a), snd.enumCase(caseName)(a))

    override def variant[A](variantId: rpcvalue.Identifier, cases: VariantCases[A]): Out[A] =
      (fst.variant(variantId, cases._1), snd.variant(variantId, cases._2))

    override def variantCase[B, A](caseName: String, o: Out[B])(inject: B => A)(
        select: A PartialFunction B): VariantCases[A] =
      (
        fst.variantCase(caseName, o._1)(inject)(select),
        snd.variantCase(caseName, o._2)(inject)(select))

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override val RecordFields: InvariantApply[RecordFields] =
      fst.RecordFields product snd.RecordFields

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override val EnumCases: Plus[EnumCases] =
      fst.EnumCases product snd.EnumCases

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override val VariantCases: Plus[VariantCases] =
      fst.VariantCases product snd.VariantCases

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override val primitive: ValuePrimitiveEncoding[Out] =
      ValuePrimitiveEncoding.product(fst.primitive, snd.primitive)
  }
}
