// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client
package binding
package encoding

import java.lang.Math.max
import java.time.{Instant, LocalDate}
import java.util.concurrent.TimeUnit

import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.v1.{value => rpcvalue}
import com.daml.ledger.client.binding.{Primitive => P}
import org.scalacheck.Shrink
import org.scalacheck.Shrink.{shrinkContainer, shrinkContainer2, shrinkFractional, shrinkIntegral}
import scalaz.{OneAnd, Plus}

import scala.math.Numeric.LongIsIntegral

abstract class ShrinkEncoding extends LfTypeEncoding {
  import ShrinkEncoding.{RecordFieldsImpl, VariantCasesImpl, primitiveImpl}

  // Shrink[A] is a typeclass version of the function:
  // A => Stream[A]
  // this LfTypeEncoding can rely heavily on the instances
  // that come with Scalacheck
  type Out[A] = Shrink[A]

  type Field[A] = Shrink[A]

  type RecordFields[A] = Shrink[A]

  type VariantCases[A] = Shrink[A] // probably

  override def record[A](recordId: rpcvalue.Identifier, fi: RecordFields[A]): Out[A] = fi

  override def emptyRecord[A](recordId: rpcvalue.Identifier, element: () => A): Out[A] =
    Shrink.shrinkAny

  override def field[A](fieldName: String, o: Out[A]): Field[A] = o

  override def fields[A](fi: Field[A]): RecordFields[A] = fi

  override def variant[A](variantId: rpcvalue.Identifier, cases: VariantCases[A]): Out[A] = cases

  @annotation.nowarn("cat=deprecation&origin=scala\\.Stream")
  override def enumAll[A](
      enumId: Identifier,
      index: A => Int,
      cases: OneAnd[Vector, (String, A)],
  ): Out[A] =
    Shrink { a: A =>
      if (index(a) == 0) Stream.empty else Stream(cases.head._2)
    }

  @annotation.nowarn("cat=deprecation&origin=scala\\.Stream")
  override def variantCase[B, A](caseName: String, o: Out[B])(
      inject: B => A
  )(select: A PartialFunction B): VariantCases[A] = Shrink[A] { a: A =>
    val ob: Option[B] = select.lift(a)
    val osa: Option[Stream[A]] = ob.map(b_ => o.shrink(b_).map(inject))
    osa.getOrElse(Stream.empty[A])
  }

  override val RecordFields: InvariantApply[RecordFields] = new RecordFieldsImpl

  override val VariantCases: Plus[VariantCases] = new VariantCasesImpl

  override val primitive: ValuePrimitiveEncoding[Out] = new primitiveImpl
}

object ShrinkEncoding extends ShrinkEncoding {
  class RecordFieldsImpl extends InvariantApply[RecordFields] {
    override def xmap[A, B](fa: RecordFields[A], f: A => B, g: B => A): RecordFields[B] =
      Shrink.xmap(f, g)(fa)

    override def xmapN[A, B, Z](fa: RecordFields[A], fb: RecordFields[B])(
        f: (A, B) => Z
    )(g: Z => (A, B)): RecordFields[Z] = {
      implicit val ifa: Shrink[A] = fa
      implicit val ifb: Shrink[B] = fb
      Shrink.xmap(f.tupled, g)
    }
  }

  class VariantCasesImpl extends Plus[VariantCases] {
    override def plus[A](a: VariantCases[A], b: => VariantCases[A]): VariantCases[A] =
      Shrink { x: A =>
        a.shrink(x) ++ b.shrink(x)
      }
  }

  class primitiveImpl extends ValuePrimitiveEncoding[Out] {
    override val valueInt64: Out[P.Int64] = shrinkIntegral

    override val valueNumeric: Out[P.Numeric] = shrinkFractional

    override val valueParty: Out[P.Party] = P.Party.subst(myShrinkString)

    override val valueText: Out[P.Text] = myShrinkString

    @annotation.nowarn("cat=deprecation&origin=scala\\.Stream")
    private def myShrinkString: Shrink[String] = Shrink { s0: String =>
      import scalaz.std.stream.unfold

      val stream: Stream[Set[String]] = unfold((Set(s0), Set.empty[String])) { case (seed, seen) =>
        if (seed.isEmpty) {
          if (seen.contains("")) None
          else {
            val as = Set("")
            Some((as, (as, seen ++ as)))
          }
        } else {
          val as: Set[String] = seed
            .flatMap { a: String =>
              if (a.length == 1) Set(a)
              else {
                val (l, r) = a.splitAt(a.length / 2)
                Set(a, l, r)
              }
            }
            .diff(seen)
          Some((as, (as, seen ++ as)))
        }
      }

      stream.flatten
    }

    override val valueDate: Out[P.Date] = Shrink { d: P.Date =>
      import scalaz.std.stream
      stream.unfold(d) { d =>
        val d1 = LocalDate.ofYearDay(d.getYear / 2, max(d.getDayOfYear / 2, 1))
        val o: Option[P.Date] = P.Date.fromLocalDate(d1)
        o.map(x => (x, x))
      }
    }

    private val nanosPerSecond: Long = TimeUnit.SECONDS.toNanos(1L)

    override val valueTimestamp: Out[P.Timestamp] = Shrink { t: P.Timestamp =>
      import scalaz.std.stream
      def reduce(d: Instant): Instant = {
        val s: Long = d.getEpochSecond
        val n: Long = d.getNano.toLong
        if (n == 0) Instant.ofEpochSecond(s / 2, nanosPerSecond - 1)
        else Instant.ofEpochSecond(s, n / 2)
      }

      stream
        .unfold(t) { t =>
          val o: Option[P.Timestamp] = P.Timestamp.discardNanos(reduce(t))
          o.map(x => (x, x))
        }
    }

    override val valueUnit: Out[P.Unit] =
      implicitly[Shrink[Unit]] // nothing to shrink, so it is shrinkAny

    override val valueBool: Out[P.Bool] = implicitly[Shrink[Boolean]] // uses shrinkAny BTW

    override def valueList[A: Out]: Out[P.List[A]] = shrinkContainer

    override def valueContractId[A]: Out[P.ContractId[A]] =
      implicitly[Shrink[P.ContractId[A]]] // don't want to shrink it, so it is shrinkAny

    override def valueOptional[A: Out]: Out[P.Optional[A]] = implicitly[Out[P.Optional[A]]]

    def entry[A: Out]: Out[(String, A)] = implicitly[Out[(String, A)]]

    override implicit def valueTextMap[A: Out]: Out[P.TextMap[A]] =
      shrinkContainer2[Lambda[(uk, v) => P.TextMap[v]], String, A]

    override implicit def valueGenMap[K: Shrink, V: Shrink]: Shrink[P.GenMap[K, V]] =
      shrinkContainer2[P.GenMap, K, V]

  }

  object Implicits {
    implicit def `Lf derived shrink`[A <: ValueRef: LfEncodable]: Shrink[A] =
      LfEncodable.encoding(ShrinkEncoding)
  }
}
