// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data._
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
            precondition True,
            signatories Nil @Party,
            observers Nil @Party,
            agreement "Agreement",
            choices { }
          } ;
        }
    """

  "unsafeCompile" should {

    val compiledPackages = assertRight(PureCompiledPackages(Map(pkgId -> pkg)))

    "handle 10k commands" in {

      val cmds = ImmArray.ImmArraySeq
        .fill(10 * 1000)(
          Command.Create(
            recordCon,
            SValue.SRecord(recordCon, ImmArray.empty, new util.ArrayList())
          )
        )
        .toImmArray

      compiledPackages.compiler.unsafeCompile(cmds) shouldBe a[SExpr]
    }
  }

}
