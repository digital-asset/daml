// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.binding.encoding.EncodingUtil.normalize
import com.daml.ledger.client.binding.{Primitive => P}
import scalaz.{OneAnd, Plus}

abstract class EqualityEncoding extends LfTypeEncoding {
  import EqualityEncoding.{RecordFieldsImpl, VariantCasesImpl, primitiveImpl}

  type Fn[A] = (A, A) => Boolean

  type Out[A] = Fn[A]

  type Field[A] = Fn[A]

  type RecordFields[A] = Fn[A]

  type VariantCases[A] = Fn[A]

  override def record[A](recordId: Identifier, fi: RecordFields[A]): Out[A] = fi

  override def emptyRecord[A](recordId: Identifier, element: () => A): Out[A] = (_, _) => true

  override def field[A](fieldName: String, o: Out[A]): Field[A] = o

  override def fields[A](fi: Field[A]): RecordFields[A] = fi

  override def enumAll[A](
      enumId: Identifier,
      index: A => Int,
      cases: OneAnd[Vector, (String, A)],
  ): Out[A] = index(_) == index(_)

  override def variant[A](variantId: Identifier, cases: VariantCases[A]): Out[A] = cases

  override def variantCase[B, A](caseName: String, o: Out[B])(
      inject: B => A
  )(select: PartialFunction[A, B]): VariantCases[A] = { (a1: A, a2: A) =>
    (select.lift(a1), select.lift(a2)) match {
      case (Some(b1), Some(b2)) => o(b1, b2)
      case (None, None) => true
      case _ => false
    }
  }

  override val RecordFields: InvariantApply[RecordFields] = new RecordFieldsImpl

  override val VariantCases: Plus[VariantCases] = new VariantCasesImpl

  override val primitive: ValuePrimitiveEncoding[Out] = new primitiveImpl
}

object EqualityEncoding extends EqualityEncoding {
  class RecordFieldsImpl extends InvariantApply[Fn] {
    def xmap[A, B](ma: Fn[A], f: A => B, g: B => A): Fn[B] = { (b1, b2) =>
      val a1 = g(b1)
      val a2 = g(b2)
      ma(a1, a2)
    }

    def xmapN[A, B, Z](fa: Fn[A], fb: Fn[B])(f: (A, B) => Z)(g: Z => (A, B)): Fn[Z] = { (z1, z2) =>
      val (a1, b1) = g(z1)
      val (a2, b2) = g(z2)
      fa(a1, a2) && fb(b1, b2)
    }
  }

  class VariantCasesImpl extends Plus[Fn] {
    def plus[A](f: Fn[A], g: => Fn[A]): Fn[A] = (a1, a2) => f(a1, a2) && g(a1, a2)
  }

  class primitiveImpl extends ValuePrimitiveEncoding[Fn] {
    override def valueInt64: Fn[P.Int64] = (a1, a2) => a1 == a2

    override def valueNumeric: Fn[P.Numeric] = (a1, a2) => a1 == a2

    override def valueParty: Fn[P.Party] = (a1, a2) => a1 == a2

    override def valueText: Fn[P.Text] = (a1, a2) => normalize(a1) == normalize(a2)

    override def valueDate: Fn[P.Date] = (a1, a2) => a1 == a2

    override def valueTimestamp: Fn[P.Timestamp] = (a1, a2) => a1 == a2

    override def valueUnit: Fn[P.Unit] = (_, _) => true

    override def valueBool: Fn[P.Bool] = (a1, a2) => a1 == a2

    override def valueList[A: Fn]: Fn[P.List[A]] = (a1, a2) => {
      val fn = implicitly[Fn[A]]
      if (a1.length != a2.length) false
      else a1.view.zip(a2.view).forall(fn.tupled)
    }

    override def valueContractId[A]: Fn[P.ContractId[A]] = (a1, a2) => a1 == a2

    override def valueOptional[A](implicit ev: Fn[A]): Fn[P.Optional[A]] = {
      case (None, None) => true
      case (Some(a1), Some(a2)) => ev(a1, a2)
      case _ => false
    }

    override implicit def valueTextMap[A: Fn]: Fn[P.TextMap[A]] = { (a1, a2) =>
      val ev = implicitly[Fn[A]]
      a1.keys == a2.keys && a1.keys.forall(k => ev(a1(k), a2(k)))
    }

    override implicit def valueGenMap[K: Fn, V: Fn]: Fn[P.GenMap[K, V]] = { (a1, a2) =>
      val evV = implicitly[Fn[V]]
      a1.keys == a2.keys && a1.keys.forall(k => evV(a1(k), a2(k)))
    }

  }

  object Implicits {
    implicit def `Lf derived equal`[A: LfEncodable]: scalaz.Equal[A] =
      scalaz.Equal.equal(LfEncodable.encoding(EqualityEncoding))
  }
}
