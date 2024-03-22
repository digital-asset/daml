// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.data.BackStack
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.language.{Ast => Pkg, Util => PkgUtil}

import scala.language.implicitConversions

class TypeSpec extends AnyWordSpec with Matchers {
  implicit def packageId(s: String): PackageId = PackageId.assertFromString(s)
  implicit def qualifiedName(s: String): QualifiedName = QualifiedName.assertFromString(s)

  implicit def fromLfPackageType(pkgTyp00: Pkg.Type): Type = {
    def assertNArg(n: Int, typs: BackStack[Type]): ImmArraySeq[Type] =
      if (typs.length == n)
        typs.toImmArray.toSeq
      else
        sys.error(s"expected #n argument, got ${typs.length}")

    def assertZeroArgs(typs: BackStack[Type]): Unit =
      if (!typs.isEmpty)
        sys.error(s"expected 0 arguments, got ${typs.toImmArray.length}")

    @scala.annotation.tailrec
    def go(typ0: Pkg.Type, args: BackStack[Type]): Type = typ0 match {
      case Pkg.TVar(v) =>
        assertZeroArgs(args)
        TypeVar(v)
      case PkgUtil.TNumeric(Pkg.TNat(n)) =>
        assertZeroArgs(args)
        TypeNumeric(n)
      case Pkg.TApp(fun, arg) => go(fun, args :+ fromLfPackageType(arg))
      case Pkg.TBuiltin(bltin) =>
        bltin match {
          case Pkg.BTInt64 =>
            assertZeroArgs(args)
            TypePrim(PrimTypeInt64, ImmArraySeq.empty)
          case Pkg.BTNumeric =>
            sys.error("cannot use Numeric not applied to TNat in interface type")
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
            TypePrim(PrimTypeList, assertNArg(1, args))
          case Pkg.BTTextMap =>
            TypePrim(PrimTypeTextMap, assertNArg(1, args))
          case Pkg.BTGenMap =>
            TypePrim(PrimTypeGenMap, assertNArg(2, args))
          case Pkg.BTUpdate =>
            sys.error("cannot use update in interface type")
          case Pkg.BTScenario =>
            sys.error("cannot use scenario in interface type")
          case Pkg.BTDate =>
            assertZeroArgs(args)
            TypePrim(PrimTypeDate, ImmArraySeq.empty)
          case Pkg.BTContractId =>
            TypePrim(PrimTypeBool, assertNArg(1, args))
          case Pkg.BTOptional => TypePrim(PrimTypeOptional, assertNArg(1, args))
          case Pkg.BTArrow => sys.error("cannot use arrow in interface type")
          case Pkg.BTAny => sys.error("cannot use any in interface type")
          case Pkg.BTTypeRep => sys.error("cannot use type representation in interface type")
          case Pkg.BTRoundingMode => sys.error("cannot use rounding mode in interface type")
          case Pkg.BTBigNumeric => sys.error("cannot use big numeric in interface type")
          case Pkg.BTAnyException =>
            sys.error("exception not supported")
        }
      case Pkg.TTyCon(tycon) => TypeCon(TypeConName(tycon), args.toImmArray.toSeq)
      case Pkg.TNat(_) => sys.error("cannot use nat type in interface type")
      case _: Pkg.TStruct => sys.error("cannot use structs in interface type")
      case _: Pkg.TForall => sys.error("cannot use forall in interface type")
      case _: Pkg.TSynApp => sys.error("cannot use type synonym in interface type")
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
        ImmArraySeq(n"a", n"b"),
        Record(ImmArraySeq(n"fld1" -> t"List a", n"fld2" -> t"Mod:V b")),
      )
    )
    inst shouldBe Record[Type](ImmArraySeq(n"fld1" -> t"List Int64", n"fld2" -> t"Mod:V Text"))
  }

  "mapTypeVars should replace all type variables in `List (List a)`" in {
    val result: Type = t"List (List a)".mapTypeVars(_ => t"Text")
    val expected: Type = t"List (List Text)"

    result shouldBe expected
  }

  "mapTypeVars should replace all type variables in `GenMap a b`" in {
    val result: Type = t"GenMap a b".mapTypeVars(_ => t"GenMap a b")
    val expected: Type = t"GenMap (GenMap a b) (GenMap a b)"

    result shouldBe expected
  }

  "instantiate should work for a nested record" in {
    val id1 = TypeConName(Identifier("P", "M:T1"))
    val id2 = TypeConName(Identifier("P", "M:T2"))

    val tc = TypeCon(id1, ImmArraySeq(t"Text"))
    val ddt = DefDataType(
      ImmArraySeq(n"a"),
      Record(
        ImmArraySeq(
          n"f" -> TypeCon(id2, ImmArraySeq(t"a"))
        )
      ),
    )
    val result = tc.instantiate(ddt)

    result shouldBe Record(ImmArraySeq(n"f" -> TypeCon(id2, ImmArraySeq(t"Text"))))
  }

}
