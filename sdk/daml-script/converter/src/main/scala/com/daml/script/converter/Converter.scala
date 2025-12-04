// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.converter

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast.Type
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import scalaz.std.either._
import scalaz.syntax.bind._

class ConverterException(message: String) extends RuntimeException(message)

object Converter {
  import Implicits._

  type ErrorOr[+A] = Either[String, A]

  def toContractId(v: Value): ErrorOr[ContractId] =
    v.expect("ContractId", { case ValueContractId(cid) => cid })

  def toText(v: Value): ErrorOr[String] =
    v.expect("Text", { case ValueText(s) => s })

  // Helper to make constructing an SRecord more convenient
  def record(ty: Identifier, fields: (String, Value)*): (Value, Type) =
    recordWithTypeArgs(ty, List.empty, fields: _*)

  def recordWithTypeArgs(
      ty: Identifier,
      tyArgs: List[Type],
      fields: (String, Value)*
  ): (Value, Type) =
    (
      ValueRecord(
        Some(ty),
        fields.view.map { case (n, v) => (Some(Name.assertFromString(n)), v) }.to(ImmArray),
      ),
      TTyConApp(ty, tyArgs.to(ImmArray)),
    )

  def makeTuple(v1: (Value, Type), v2: (Value, Type)): (Value, Type) =
    recordWithTypeArgs(StablePackagesV2.Tuple2, List(v1._2, v2._2), ("_1", v1._1), ("_2", v2._1))

  def makeTuple(v1: (Value, Type), v2: (Value, Type), v3: (Value, Type)): (Value, Type) =
    recordWithTypeArgs(
      StablePackagesV2.Tuple3,
      List(v1._2, v2._2, v3._2),
      ("_1", v1._1),
      ("_2", v2._1),
      ("_3", v3._1),
    )

  private[this] val DaTypesTuple2 =
    QualifiedName(DottedName.assertFromString("DA.Types"), DottedName.assertFromString("Tuple2"))

  object DamlTuple2 {
    def unapply(v: ValueRecord): Option[(Value, Value)] = v match {
      case ValueRecord(Some(Identifier(_, DaTypesTuple2)), ImmArray((_, fst), (_, snd))) =>
        Some((fst, snd))
      case _ => None
    }
  }

  object DamlAnyModuleRecord {
    def unapplySeq(v: ValueRecord): Option[(String, collection.Seq[Value])] = v match {
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
