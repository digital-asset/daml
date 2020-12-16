// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package speedy

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.concurrent.ScalaFutures

class ScenarioRunnerTest extends AsyncWordSpec with Matchers with ScalaFutures {

  "ScenarioRunner" can {
    "mangle party names correctly" in {
      val compiledPackages = PureCompiledPackages(Map.empty).right.get
      val e = Ast.EScenario(Ast.ScenarioGetParty(Ast.EPrimLit(Ast.PLText("foo-bar"))))
      val txSeed = crypto.Hash.hashPrivateKey("ScenarioRunnerTest")
      val m = Speedy.Machine.fromScenarioExpr(
        compiledPackages,
        txSeed,
        e,
      )
      val sr = ScenarioRunner(m, _ + "-XXX")
      sr.run() match {
        case Right((_, _, _, value)) =>
          value shouldBe SValue.SParty(Ref.Party.assertFromString("foo-bar-XXX"))
        case res =>
          sys.error(s"unexpected result $res")
      }
    }
  }

}
