// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding
package encoding

import scala.language.higherKinds
import scala.collection.immutable.Seq
import scalaz.Isomorphism.<~>

import com.digitalasset.ledger.api.v1.value.Value.{Sum => VSum}
import com.digitalasset.ledger.client.binding.{Primitive => P}

/** Core instances of a typeclass (or other type) that relate all cases of the
  * [[com.digitalasset.ledger.api.v1.value.Value]] sum type to the [[Primitive]]
  * types.  All other instances, such as those for record, sum, and template
  * types, are derived from these ones, which thus form the "axioms" of any
  * typeclass relating gRPC values to custom DAML data types built upon Scala
  * datatypes.
  */
trait ValuePrimitiveEncoding[TC[_]] {
  implicit def valueInt64: TC[P.Int64]

  implicit def valueDecimal: TC[P.Decimal]

  implicit def valueParty: TC[P.Party]

  implicit def valueText: TC[P.Text]

  implicit def valueDate: TC[P.Date]

  implicit def valueTimestamp: TC[P.Timestamp]

  implicit def valueUnit: TC[P.Unit]

  implicit def valueBool: TC[P.Bool]

  implicit def valueList[A: TC]: TC[P.List[A]]

  implicit def valueContractId[Tpl <: Template[Tpl]]: TC[P.ContractId[Tpl]]

  implicit def valueOptional[A: TC]: TC[P.Optional[A]]

  implicit def valueMap[A: TC]: TC[P.Map[A]]
}

object ValuePrimitiveEncoding {
  private[this] trait CidT extends Template[CidT]

  /** Proof that `ValuePrimitiveEncoding`, and therefore the primitive types and
    * Value, have an instance for any possible primitive value.
    */
  private[binding] def coreInstance[TC[_]](te: ValuePrimitiveEncoding[TC])(
      sCase: VSum): Option[TC[_]] = {
    import te._, VSum._
    sCase match {
      // if you remove a case here, also delete the member referenced on the RHS
      // from `trait ValuePrimitiveEncoding`, and all its implementations
      case Int64(_) => Some(valueInt64)
      case Decimal(_) => Some(valueDecimal)
      case Party(_) => Some(valueParty)
      case Text(_) => Some(valueText)
      case Date(_) => Some(valueDate)
      case Timestamp(_) => Some(valueTimestamp)
      case Unit(_) => Some(valueUnit)
      case Bool(_) => Some(valueBool)
      case List(_) => Some(valueList(valueText))
      case ContractId(_) => Some(valueContractId[CidT])
      case Optional(_) => Some(valueOptional(valueText))
      case Map(_) => Some(valueMap(valueText))
      // types that represent non-primitives only
      case Record(_) | Variant(_) | Empty => None
    }
  }

  def roots[TC[_]](te: ValuePrimitiveEncoding[TC]): Seq[TC[_]] = {
    import te._
    Seq(
      valueInt64,
      valueDecimal,
      valueParty,
      valueText,
      valueDate,
      valueTimestamp,
      valueUnit,
      valueBool,
      valueContractId[CidT])
  }

  // def const[F[_]](fa: Forall[F]): ValuePrimitiveEncoding[F] =

  def product[F[_], G[_]](
      vpef: ValuePrimitiveEncoding[F],
      vpeg: ValuePrimitiveEncoding[G]): ValuePrimitiveEncoding[Lambda[a => (F[a], G[a])]] =
    new ValuePrimitiveEncoding[Lambda[a => (F[a], G[a])]] {
      override def valueInt64 = (vpef.valueInt64, vpeg.valueInt64)

      override def valueDecimal = (vpef.valueDecimal, vpeg.valueDecimal)

      override def valueParty = (vpef.valueParty, vpeg.valueParty)

      override def valueText = (vpef.valueText, vpeg.valueText)

      override def valueDate = (vpef.valueDate, vpeg.valueDate)

      override def valueTimestamp = (vpef.valueTimestamp, vpeg.valueTimestamp)

      override def valueUnit = (vpef.valueUnit, vpeg.valueUnit)

      override def valueBool = (vpef.valueBool, vpeg.valueBool)

      override def valueList[A](implicit ev: (F[A], G[A])) =
        (vpef.valueList(ev._1), vpeg.valueList(ev._2))

      override def valueContractId[Tpl <: Template[Tpl]] =
        (vpef.valueContractId[Tpl], vpeg.valueContractId[Tpl])

      override def valueOptional[A](implicit ev: (F[A], G[A])) =
        (vpef.valueOptional(ev._1), vpeg.valueOptional(ev._2))

      override def valueMap[A](implicit ev: (F[A], G[A])) =
        (vpef.valueMap(ev._1), vpeg.valueMap(ev._2))
    }

  /** Transforms all the base cases of `F` to `G`, leaving the inductive cases
    * abstract.
    */
  trait Mapped[F[_], G[_]] extends ValuePrimitiveEncoding[G] {
    protected[this] def fgAxiom[A](fa: F[A]): G[A]
    protected[this] def underlyingVpe: ValuePrimitiveEncoding[F]

    override final def valueInt64: G[P.Int64] = fgAxiom(underlyingVpe.valueInt64)

    override final def valueDecimal: G[P.Decimal] = fgAxiom(underlyingVpe.valueDecimal)

    override final def valueParty: G[P.Party] = fgAxiom(underlyingVpe.valueParty)

    override final def valueText: G[P.Text] = fgAxiom(underlyingVpe.valueText)

    override final def valueDate: G[P.Date] = fgAxiom(underlyingVpe.valueDate)

    override final def valueTimestamp: G[P.Timestamp] = fgAxiom(underlyingVpe.valueTimestamp)

    override final def valueUnit: G[P.Unit] = fgAxiom(underlyingVpe.valueUnit)

    override final def valueBool: G[P.Bool] = fgAxiom(underlyingVpe.valueBool)
  }

  /** Transform all cases to a new type.
    *
    * @note Technically we want a higher-kinded ''lens'' here, not a higher-kinded
    *       isomorphism (which is a subset of HK lenses), but we don't have monocle
    *       imported locally.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def xmapped[F[_], G[_]](vpe: ValuePrimitiveEncoding[F])(iso: F <~> G): ValuePrimitiveEncoding[G] =
    new Mapped[F, G] {
      override protected[this] def fgAxiom[A](fa: F[A]) = iso.to(fa)
      override protected[this] def underlyingVpe = vpe

      override def valueList[A](implicit ev: G[A]): G[P.List[A]] =
        iso.to(vpe.valueList(iso.from(ev)))

      override def valueContractId[Tpl <: Template[Tpl]]: G[P.ContractId[Tpl]] =
        iso.to(vpe.valueContractId)

      override def valueOptional[A](implicit ev: G[A]): G[P.Optional[A]] =
        iso.to(vpe.valueOptional(iso.from(ev)))

      override def valueMap[A](implicit ev: G[A]): G[P.Map[A]] = iso.to(vpe.valueMap(iso.from(ev)))
    }
}
