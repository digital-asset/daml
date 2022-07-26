// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion, PackageInterface}

package object validation {

  val V1 = OldTyping
  val V2 = NewTyping

  object Typing {
    val checkModule = V1.checkModule _

    var i = 0

    case class Env(
        languageVersion: LanguageVersion,
        pkgInterface: PackageInterface,
        ctx: Context,
        tVars: Map[TypeVarName, Kind] = Map.empty,
        eVars: Map[ExprVarName, Type] = Map.empty,
    ) {

      def env1 = V1.Env(languageVersion, pkgInterface, ctx, tVars, eVars)
      def env2 = V2.Env(languageVersion, pkgInterface, ctx, tVars, eVars)

      def expandTypeSynonyms(t: Type): Type = env1.expandTypeSynonyms(t)
      def checkKind(k: Kind): Unit = env1.checkKind(k)
      def kindOf(t: Type): Kind = env1.kindOf(t)

      def env1_wrapped_typeOf(e: Expr): Either[Throwable, Type] = {
        try Right(env1.typeOf(e))
        catch { case ex: Throwable => Left(ex) }
      }

      def env2_wrapped_typeOf(e: Expr): Either[Throwable, Type] = {
        try Right(env2.xtypeOf(e))
        catch { case ex: Throwable => Left(ex) }
      }

      def wrapped_typeOf(e: Expr): Either[Throwable, Type] = {
        i = i + 1
        val v1 = env1_wrapped_typeOf(e)
        val v2 = env2_wrapped_typeOf(e)
        if (v1 == v2) {
          // println(s"**($i) SAME")
        } else {
          println(s"**($i) typeOf: $e\n  - v1: $v1\n  - v2: $v2")
        }
        v1
      }

      def typeOf(e: Expr): Type = {
        wrapped_typeOf(e) match {
          case Left(ex) =>
            throw ex
          case Right(t) =>
            t
        }
      }

    }

  }

}
