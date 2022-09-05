// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SExpr.SExpr
import com.daml.lf.testing.parser.Implicits._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompilerTest extends AnyWordSpec with Matchers {

  import defaultParserParameters.{defaultPackageId => pkgId}

  private[this] val recordCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Record"))

  private[this] val pkg =
    p"""
        module Module {

          record @serializable Record = { };

          template (this : Record) =  {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            agreement "Agreement";
          } ;
        }
    """

  "unsafeCompile" should {

    val compiledPackages = PureCompiledPackages.assertBuild(Map(pkgId -> pkg))

    "handle 10k commands" in {

      val cmds = ImmArray.ImmArraySeq
        .fill(10 * 1000)(
          Command.Create(
            recordCon,
            SValue.SRecord(recordCon, ImmArray.Empty, ArrayList.empty),
          )
        )
        .toImmArray

      compiledPackages.compiler.unsafeCompile(cmds) shouldBe a[SExpr]
    }

    "compile deeply nested lets" in {
      val expr = List
        .range[Long](1, 3000)
        .foldRight[Expr](EPrimLit(PLInt64(5000)))((i, acc) =>
          ELet(
            Binding(
              Some(Ref.Name.assertFromString(s"v$i")),
              TBuiltin(BTInt64),
              EPrimLit(PLInt64(i)),
            ),
            acc,
          )
        )

      compiledPackages.compiler.unsafeCompile(expr)
    }
  }

}
