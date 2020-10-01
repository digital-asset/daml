// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.converter

// import io.grpc.StatusRuntimeException
import java.util

// import scala.annotation.tailrec
import scala.collection.JavaConverters._
/*
import scala.concurrent.Future
import scalaz.{-\/, \/-}
import spray.json._
 */
import scalaz.syntax.bind._
import scalaz.std.either._
import com.daml.lf.data.{ImmArray, Ref /*, Struct, Time*/}
import Ref._
/*
import com.daml.lf.iface
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
 */
import com.daml.lf.language.Ast
/*
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
 */
import com.daml.lf.speedy.{/*Pretty, SExpr, */ SValue /*, Speedy*/}
// import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
// import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
/*
import com.daml.lf.CompiledPackages
import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.api.v1.value
 */

class ConverterException(message: String) extends RuntimeException(message)

private[daml] object Converter {
  import Implicits._

  type ErrorOr[+A] = Either[String, A]

  private val DA_INTERNAL_ANY_PKGID =
    PackageId.assertFromString("cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7")
  def daInternalAny(s: String): Identifier =
    Identifier(
      DA_INTERNAL_ANY_PKGID,
      QualifiedName(DottedName.assertFromString("DA.Internal.Any"), DottedName.assertFromString(s)))

  def toContractId(v: SValue): ErrorOr[ContractId] =
    v expect ("ContractId", { case SContractId(cid) => cid })

  def toText(v: SValue): ErrorOr[String] =
    v expect ("SText", { case SText(s) => s })

  // Helper to make constructing an SRecord more convenient
  def record(ty: Identifier, fields: (String, SValue)*): SValue = {
    val fieldNames = fields.iterator.map { case (n, _) => Name.assertFromString(n) }.to[ImmArray]
    val args =
      new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
    SRecord(ty, fieldNames, args)
  }

  /** Unpack one step of a Pure/Roll-style free monad representation,
    * with the assumption that `f` is a variant type.
    */
  def unrollFree(v: SValue): ErrorOr[SValue Either (Ast.VariantConName, SValue)] =
    v expect ("Free with variant or Pure", {
      case SVariant(_, "Free", _, SVariant(_, variant, _, vv)) =>
        Right((variant, vv))
      case SVariant(_, "Pure", _, v) => Left(v)
    })

  private[this] val DaTypesTuple2 =
    QualifiedName(DottedName.assertFromString("DA.Types"), DottedName.assertFromString("Tuple2"))

  object DamlTuple2 {
    def unapply(v: SRecord): Option[(SValue, SValue)] = v match {
      case SRecord(Identifier(_, DaTypesTuple2), _, JavaList(fst, snd)) =>
        Some((fst, snd))
      case _ => None
    }
  }

  object JavaList {
    def unapplySeq[A](jl: util.List[A]): Some[Seq[A]] =
      Some(jl.asScala)
  }

  object Implicits {
    implicit final class `intoOr and expect`[A](private val self: A) extends AnyVal {
      def intoOr[R, L](pf: A PartialFunction R)(orElse: => L): Either[L, R] =
        pf.lift(self) toRight orElse

      def expect[R](name: String, pf: A PartialFunction R): ErrorOr[R] =
        self.intoOr(pf)(s"Expected $name but got $self")

      def expectE[R](name: String, pf: A PartialFunction ErrorOr[R]): ErrorOr[R] =
        self.expect(name, pf).join
    }
  }
}
