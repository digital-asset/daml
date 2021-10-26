// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package scenario

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.concurrent.ScalaFutures

class ScenarioRunnerTest extends AsyncWordSpec with Matchers with ScalaFutures {

  "ScenarioRunner" can {
    "mangle party names correctly" in {
      val e = Ast.EScenario(Ast.ScenarioGetParty(Ast.EPrimLit(Ast.PLText("foo-bar"))))
      val txSeed = crypto.Hash.hashPrivateKey("ScenarioRunnerTest")
      val m = speedy.Speedy.Machine.fromScenarioExpr(PureCompiledPackages.Empty, e)
      val sr = new ScenarioRunner(m, txSeed, _ + "-XXX")
      sr.run() match {
        case success: ScenarioRunner.ScenarioSuccess =>
          success.resultValue shouldBe SValue.SParty(Ref.Party.assertFromString("foo-bar-XXX"))
        case error: ScenarioRunner.ScenarioError =>
          sys.error(s"unexpected result ${error.error}")
      }
    }
  }

}
