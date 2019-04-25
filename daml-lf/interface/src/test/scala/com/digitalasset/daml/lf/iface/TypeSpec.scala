// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.iface

import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref.{Identifier, QualifiedName, SimpleString}
import com.digitalasset.daml.lf.data.BackStack
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.lfpackage.{Ast => Pkg}

import scala.language.implicitConversions

class TypeSpec extends WordSpec with Matchers {
  implicit def simpleString(s: String): SimpleString = SimpleString.assertFromString(s)
  implicit def qualifiedName(s: String): QualifiedName = QualifiedName.assertFromString(s)

  implicit def fromLfPackageType(pkgTyp00: Pkg.Type): Type = {
    def assertOneArg(typs: BackStack[Type]): Type = typs.pop match {
      case Some((head, tail)) if head.isEmpty => tail
      case _ => sys.error(s"expected 1 argument, got ${typs.toImmArray.length}")
    }
    def assertZeroArgs(typs: BackStack[Type]): Unit = if (!typs.isEmpty) {
      sys.error(s"expected 0 arguments, got ${typs.toImmArray.length}")
    }

    def go(typ0: Pkg.Type, args: BackStack[Type]): Type = typ0 match {
      case Pkg.TVar(v) =>
        assertZeroArgs(args)
        TypeVar(v)
      case Pkg.TApp(fun, arg) => go(fun, args :+ fromLfPackageType(arg))
      case Pkg.TBuiltin(bltin) =>
        bltin match {
          case Pkg.BTInt64 =>
            assertZeroArgs(args)
            TypePrim(PrimTypeInt64, ImmArraySeq.empty)
          case Pkg.BTDecimal =>
            assertZeroArgs(args)
            TypePrim(PrimTypeDecimal, ImmArraySeq.empty)
          case Pkg.BTText =>
            assertZeroArgs(args)
            TypePrim(PrimTypeText, ImmArraySeq.empty)
          case Pkg.BTTimestamp =>
            assertZeroArgs(args)
            TypePrim(PrimTypeTimestamp, ImmArraySeq.empty)
          case Pkg.BTParty =>
            assertZeroArgs(args)
            TypePrim(PrimTypeParty, ImmArraySeq.empty)
          case Pkg.BTUnit =>
            assertZeroArgs(args)
            TypePrim(PrimTypeUnit, ImmArraySeq.empty)
          case Pkg.BTBool =>
            assertZeroArgs(args)
            TypePrim(PrimTypeBool, ImmArraySeq.empty)
          case Pkg.BTList =>
            TypePrim(PrimTypeList, ImmArraySeq(assertOneArg(args)))
          case Pkg.BTMap =>
            TypePrim(PrimTypeMap, ImmArraySeq(assertOneArg(args)))
          case Pkg.BTUpdate =>
            sys.error("cannot use update in interface type")
          case Pkg.BTScenario =>
            sys.error("cannot use scenario in interface type")
          case Pkg.BTDate =>
            assertZeroArgs(args)
            TypePrim(PrimTypeDate, ImmArraySeq.empty)
          case Pkg.BTContractId =>
            TypePrim(PrimTypeBool, ImmArraySeq(assertOneArg(args)))
          case Pkg.BTOptional => TypePrim(PrimTypeOptional, ImmArraySeq(assertOneArg(args)))
          case Pkg.BTArrow => sys.error("cannot use arrow in interface type")
        }
      case Pkg.TTyCon(tycon) => TypeCon(TypeConName(tycon), args.toImmArray.toSeq)
      case _: Pkg.TTuple => sys.error("cannot use tuples in interface type")
      case _: Pkg.TForall => sys.error("cannot use forall in interface type")
    }

    go(pkgTyp00, BackStack.empty)
  }

  "instantiate type arguments correctly" in {
    val tyCon = TypeCon(
      TypeConName(Identifier("dummyPkg", "Mod:R")),
      ImmArraySeq(t"Int64", t"Text"),
    )
    val inst = tyCon.instantiate(
      DefDataType(
        ImmArraySeq("a", "b"),
        Record(ImmArraySeq("fld1" -> t"List a", "fld2" -> t"Mod:V b"))
      )
    )
    inst shouldBe Record[Type](ImmArraySeq("fld1" -> t"List Int64", "fld2" -> t"Mod:V Text"))
  }

  "mapTypeVars should replace all type variables in List(List a)" in {
    val result: Type = t"List (List a)".mapTypeVars(v => t"Text")
    val expected: Type = t"List (List Text)"

    result shouldBe expected
  }

  "instantiate should work for a nested record" in {
    val id1 = TypeConName(Identifier("P", "M:T1"))
    val id2 = TypeConName(Identifier("P", "M:T2"))

    val tc = TypeCon(id1, ImmArraySeq(t"Text"))
    val ddt = DefDataType(
      ImmArraySeq("a"),
      Record(
        ImmArraySeq(
          "f" -> TypeCon(id2, ImmArraySeq(t"a"))
        ))
    )
    val result = tc.instantiate(ddt)

    result shouldBe Record(ImmArraySeq("f" -> TypeCon(id2, ImmArraySeq(t"Text"))))
  }
}
