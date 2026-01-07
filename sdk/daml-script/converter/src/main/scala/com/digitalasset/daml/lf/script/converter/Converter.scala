// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script.converter

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.engine.ScriptEngine.ExtendedValue
import com.digitalasset.daml.lf.value.Value._
import scalaz.std.either._
import scalaz.syntax.bind._

class ConverterException(message: String) extends RuntimeException(message)

object Converter {
  import Implicits._

  type ErrorOr[+A] = Either[String, A]

  def toContractId(v: ExtendedValue): ErrorOr[ContractId] =
    v.expect("ContractId", { case ValueContractId(cid) => cid })

  def toText(v: ExtendedValue): ErrorOr[String] =
    v.expect("Text", { case ValueText(s) => s })

  // Helper to make constructing a ValueRecord more convenient
  def record(ty: Identifier, fields: (String, ExtendedValue)*): ExtendedValue =
    ValueRecord(
      Some(ty),
      fields.view.map { case (n, v) => (Some(Name.assertFromString(n)), v) }.to(ImmArray),
    )

  def makeTuple(v1: ExtendedValue, v2: ExtendedValue): ExtendedValue =
    record(StablePackagesV2.Tuple2, ("_1", v1), ("_2", v2))

  def makeTuple(v1: ExtendedValue, v2: ExtendedValue, v3: ExtendedValue): ExtendedValue =
    record(StablePackagesV2.Tuple3, ("_1", v1), ("_2", v2), ("_3", v3))

  private[this] val DaTypesTuple2 =
    QualifiedName(DottedName.assertFromString("DA.Types"), DottedName.assertFromString("Tuple2"))

  object DamlTuple2 {
    def unapply(v: ValueRecord): Option[(ExtendedValue, ExtendedValue)] = v match {
      case ValueRecord(Some(Identifier(_, DaTypesTuple2)), ImmArray((_, fst), (_, snd))) =>
        Some((fst, snd))
      case _ => None
    }
  }

  object DamlAnyModuleRecord {
    def unapplySeq(v: ValueRecord): Option[(String, collection.Seq[ExtendedValue])] = v match {
      case ValueRecord(Some(Identifier(_, QualifiedName(_, name))), fields) =>
        Some((name.dottedName, fields.map(_._2).toSeq))
      case _ => None
    }
  }

  object Implicits {
    implicit final class `intoOr and expect`[A](private val self: A) extends AnyVal {
      def intoOr[R, L](pf: A PartialFunction R)(orElse: => L): Either[L, R] =
        pf.lift(self) toRight orElse

      def expect[R](name: String, pf: A PartialFunction R): ErrorOr[R] = {
        if (name == self.getClass.getSimpleName) {
          self.intoOr(pf)(s"Expected $name but partial function was undefined")
        } else {
          self.intoOr(pf)(s"Expected $name but got ${self.getClass.getSimpleName}")
        }
      }

      def expectE[R](name: String, pf: A PartialFunction ErrorOr[R]): ErrorOr[R] =
        self.expect(name, pf).join
    }

    implicit final class `ErrorOr ops`[A](private val self: ErrorOr[A]) extends AnyVal {
      @throws[ConverterException]
      def orConverterException: A = self.fold(e => throw new ConverterException(e), identity)
    }
  }
}
