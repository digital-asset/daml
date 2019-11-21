// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client
package binding
package encoding

import com.digitalasset.ledger.api.v1.{value => rpcvalue}
import com.digitalasset.ledger.client.binding.{Primitive => P}
import com.digitalasset.ledger.api.refinements.ApiTypes
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import scalaz.{OneAnd, Plus}
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.Isomorphism.{<~>, IsoFunctorTemplate}
import scalaz.std.vector._
import scalaz.syntax.foldable._
import scalaz.syntax.plus._

abstract class GenEncoding extends LfTypeEncoding {
  import GenEncoding.{VariantCasesImpl, primitiveImpl}

  type Out[A] = Gen[A]

  type Field[A] = Gen[A]

  type RecordFields[A] = Gen[A]

  type VariantCases[A] = OneAnd[Vector, Gen[A]]

  override def record[A](recordId: rpcvalue.Identifier, fi: RecordFields[A]): Out[A] = fi

  override def emptyRecord[A](recordId: rpcvalue.Identifier, element: () => A): Out[A] =
    Gen.const(element())

  override def field[A](fieldName: String, o: Out[A]): Field[A] = o

  override def fields[A](fi: Field[A]): RecordFields[A] = fi

  override def enumAll[A](
      enumId: rpcvalue.Identifier,
      index: A => Int,
      cases: OneAnd[Vector, (String, A)],
  ): Out[A] =
    Gen.oneOf(cases.toVector.map(_._2))

  override def variant[A](variantId: rpcvalue.Identifier, cases: VariantCases[A]): Out[A] =
    cases.tail match {
      case second +: rest => Gen.oneOf(cases.head, second, rest: _*)
      case _ => cases.head
    }

  override def variantCase[B, A](caseName: String, o: Out[B])(inject: B => A)(
      select: A PartialFunction B): VariantCases[A] =
    OneAnd(o map inject, Vector.empty)

  override val primitive: ValuePrimitiveEncoding[Gen] = new primitiveImpl

  override val RecordFields: InvariantApply[RecordFields] = InvariantApply.fromApply[RecordFields]

  override val VariantCases: Plus[VariantCases] = new VariantCasesImpl
}

object GenEncoding extends GenEncoding {

  class VariantCasesImpl extends Plus[VariantCases] {
    def plus[A](a: VariantCases[A], b: => VariantCases[A]): VariantCases[A] =
      a <+> b
  }

  class primitiveImpl extends ValuePrimitiveEncoding[Out] {
    override val valueInt64: Out[P.Int64] = arbitrary[P.Int64]

    override val valueNumeric: Out[P.Numeric] = arbitrary[BigDecimal](DamlDecimalGen.arbDamlDecimal)

    override val valueParty: Out[P.Party] = P.Party.subst(Gen.identifier)

    override val valueText: Out[P.Text] = arbitrary[P.Text]

    override val valueDate: Out[P.Date] = DamlDateGen.genDamlDate

    override val valueTimestamp: Out[P.Timestamp] = DamlTimestampGen.genDamlTimestamp

    override val valueUnit: Out[P.Unit] = arbitrary[P.Unit]

    override val valueBool: Out[P.Bool] = arbitrary[P.Bool]

    override def valueList[A](implicit ev: Out[A]): Out[P.List[A]] = {
      implicit val elt: Arbitrary[A] = Arbitrary(ev)
      arbitrary[P.List[A]]
    }

    override def valueContractId[A]: Out[P.ContractId[A]] =
      P.substContractId[Gen, A](ApiTypes.ContractId.subst(Gen.identifier))

    override def valueOptional[A](implicit ev: Out[A]): Out[P.Optional[A]] = {
      implicit val elt: Arbitrary[A] = Arbitrary(ev)
      arbitrary[P.Optional[A]]
    }

    override def valueTextMap[A](implicit ev: Out[A]): Out[P.TextMap[A]] = {
      implicit val elt: Arbitrary[A] = Arbitrary(ev)
      arbitrary[P.TextMap[A]]
    }

    override def valueGenMap[K, V](implicit evK: Out[K], evV: Out[V]): Out[P.GenMap[K, V]] = {
      implicit val eltK: Arbitrary[K] = Arbitrary(evK)
      implicit val eltV: Arbitrary[V] = Arbitrary(evV)
      arbitrary[P.GenMap[K, V]]
    }
  }

  val genArbitraryIso: Gen <~> Arbitrary = new IsoFunctorTemplate[Gen, Arbitrary] {
    override def to[A](ga: Gen[A]) = Arbitrary(ga)
    override def from[A](aa: Arbitrary[A]) = aa.arbitrary
  }

  private[this] class PostgresSafe extends GenEncoding {
    override val primitive = new primitiveImpl {
      // PostgreSQL does not like \u0000
      override val valueText: Out[P.Text] =
        arbitrary[P.Text].map(s => s.replace("\u0000", "\u0001"))
    }
  }

  val postgresSafe: GenEncoding = new PostgresSafe

  object Implicits {
    implicit def `Lf derived arbitrary`[A <: ValueRef: LfEncodable]: Arbitrary[A] =
      Arbitrary(LfEncodable.encoding(GenEncoding))
  }
}
