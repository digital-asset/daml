// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding
package encoding

import scala.language.higherKinds
import scala.collection.immutable.Seq
import scalaz.~>
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

  /** Transform all the base cases to a new type. */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def xmapped[F[_], G[_]](vpe: ValuePrimitiveEncoding[F])(iso: F <~> G): ValuePrimitiveEncoding[G] =
    mapped(vpe)(iso.to)(Lambda[G ~> Lambda[a => G[P.List[a]]]](ga =>
      iso.to(vpe.valueList(iso.from(ga)))))(Lambda[G ~> Lambda[a => G[P.Optional[a]]]](ga =>
      iso.to(vpe.valueOptional(iso.from(ga)))))(Lambda[G ~> Lambda[a => G[P.Map[a]]]](ga =>
      iso.to(vpe.valueMap(iso.from(ga)))))

  /** Transform all the base cases to a new type, supplying a fresh version of
    * `valueList` (the only inductive case, and therefore trickier).
    */
  def mapped[F[_], G[_]](vpe: ValuePrimitiveEncoding[F])(f: F ~> G)(
      newList: G ~> Lambda[a => G[P.List[a]]])(newOptional: G ~> Lambda[a => G[P.Optional[a]]])(
      newMap: G ~> Lambda[a => G[P.Map[a]]]
  ): ValuePrimitiveEncoding[G] =
    new ValuePrimitiveEncoding[G] {
      override def valueInt64: G[P.Int64] = f(vpe.valueInt64)

      override def valueDecimal: G[P.Decimal] = f(vpe.valueDecimal)

      override def valueParty: G[P.Party] = f(vpe.valueParty)

      override def valueText: G[P.Text] = f(vpe.valueText)

      override def valueDate: G[P.Date] = f(vpe.valueDate)

      override def valueTimestamp: G[P.Timestamp] = f(vpe.valueTimestamp)

      override def valueUnit: G[P.Unit] = f(vpe.valueUnit)

      override def valueBool: G[P.Bool] = f(vpe.valueBool)

      override def valueList[A](implicit ev: G[A]): G[P.List[A]] = newList(ev)

      override def valueContractId[Tpl <: Template[Tpl]]: G[P.ContractId[Tpl]] =
        f(vpe.valueContractId[Tpl])

      override def valueOptional[A](implicit ev: G[A]): G[P.Optional[A]] = newOptional(ev)

      override def valueMap[A](implicit ev: G[A]): G[P.Map[A]] = newMap(ev)
    }
}
