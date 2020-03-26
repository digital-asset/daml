// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.binding.{Primitive => P}
import scalaz.Cord.stringToCord
import scalaz.Scalaz._
import scalaz.Show.{show, showFromToString}
import scalaz._
import scalaz.std.iterable.iterableShow

abstract class ShowEncoding extends LfTypeEncoding {
  import ShowEncoding.{RecordFieldsImpl, VariantCasesImpl, primitiveImpl}

  type Out[A] = Show[A]

  type Field[A] = Show[A]

  type RecordFields[A] = Show[A]

  type VariantCases[A] = Show[A]

  private def name(id: Identifier) =
    s"${id.moduleName}.${id.entityName}"

  override def record[A](recordId: Identifier, fi: RecordFields[A]): Out[A] =
    show { a: A =>
      Cord(name(recordId), "(", fi.show(a), ")")
    }

  override def emptyRecord[A](recordId: Identifier, element: () => A): Out[A] = {
    val shown = Cord(name(recordId), "()")
    show { _: A =>
      shown
    }
  }

  override def field[A](fieldName: String, o: Out[A]): Field[A] =
    show { a: A =>
      Cord(fieldName, " = ", o.show(a))
    }

  override def fields[A](fi: Field[A]): RecordFields[A] = fi

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def enumAll[A](
      enumId: Identifier,
      index: A => Int,
      cases: OneAnd[Vector, (String, A)],
  ): Out[A] =
    show { a: A =>
      cases.index(index(a)).fold(Cord.empty)(_._1)
    }

  override def variant[A](variantId: Identifier, cases: VariantCases[A]): Out[A] = cases

  override def variantCase[B, A](caseName: String, o: Out[B])(inject: B => A)(
      select: A PartialFunction B): VariantCases[A] =
    show { a: A =>
      select.lift(a).fold(Cord.empty)(b => Cord(caseName, "(", o.show(b), ")"))
    }

  override val RecordFields: InvariantApply[RecordFields] = new RecordFieldsImpl

  override val VariantCases: Plus[VariantCases] = new VariantCasesImpl

  override val primitive: ValuePrimitiveEncoding[Out] = new primitiveImpl
}

object ShowEncoding extends ShowEncoding {
  class RecordFieldsImpl extends InvariantApply[RecordFields] {
    def xmap[A, B](ma: Show[A], f: A => B, g: B => A): Show[B] = show { b: B =>
      val a: A = g(b)
      ma.show(a)
    }

    def xmapN[A, B, Z](fa: Show[A], fb: Show[B])(f: (A, B) => Z)(g: Z => (A, B)): Show[Z] = show {
      z: Z =>
        val (a, b) = g(z)
        Cord(fa.show(a), ", ", fb.show(b))
    }
  }

  class VariantCasesImpl extends Plus[VariantCases] {
    def plus[A](a: Show[A], b: => Show[A]): Show[A] = new Show[A] {
      override def show(f: A): Cord = a.show(f) ++ b.show(f)
    }
  }

  class primitiveImpl extends ValuePrimitiveEncoding[Out] {
    private val timestampFormatter =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.nX").withZone(ZoneId.of("UTC"))

    override def valueInt64: Show[P.Int64] = longInstance

    override def valueNumeric: Show[P.Numeric] = bigDecimalInstance

    override def valueParty: Show[P.Party] =
      P.Party.subst(show(p => Cord("P@", ShowUnicodeEscapedString.show(p))))

    override def valueText: Show[P.Text] = ShowUnicodeEscapedString

    override def valueDate: Show[P.Date] = showFromToString

    override def valueTimestamp: Show[P.Timestamp] = show(t => Cord(timestampFormatter.format(t)))

    override def valueUnit: Show[P.Unit] = unitInstance

    override def valueBool: Show[P.Bool] = booleanInstance

    override def valueList[A: Show]: Show[P.List[A]] = iterableShow

    override def valueContractId[A]: Show[P.ContractId[A]] =
      P.ContractId.subst(show(s => Cord("CID@", showFromToString.show(s)))) // generated by Gen.identifier

    override def valueOptional[A: Show]: Show[P.Optional[A]] = optionShow

    override def valueTextMap[A: Show]: Show[P.TextMap[A]] = P.TextMap.leibniz[A].subst(mapShow)

    override def valueGenMap[K: Show, V: Show]: Show[P.GenMap[K, V]] = P.GenMap.insertMapShow

  }

  object Implicits {
    implicit def `Lf derived show`[A: LfEncodable]: Show[A] =
      LfEncodable.encoding(ShowEncoding)
  }
}

object ShowUnicodeEscapedString extends Show[String] {
  override def show(s: String): Cord = {
    val buf = s
      .foldLeft(new StringBuilder("\"")) { (b, a) =>
        val c: Int = a.toInt
        if (isAsciiPrintable(c)) b.append(a)
        else b.append("\\u").append("%04X".format(c))
      }
      .append("\"")
    Cord(buf.toString)
  }

  private def isAsciiPrintable(c: Int): Boolean = c != 0x5c && 0x20 <= c && c <= 0x7E
}
