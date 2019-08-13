// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast.ScenarioGetParty
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class ScenarioRunnerTest extends AsyncWordSpec with Matchers with ScalaFutures {

  "ScenarioRunner" can {
    "mangle party names correctly" in {
      val e = Ast.EScenario(ScenarioGetParty(Ast.EPrimLit(Ast.PLText(("foo-bar")))))
      val m = Speedy.Machine.fromExpr(
        expr = e,
        checkSubmitterInMaintainers = true,
        compiledPackages = PureCompiledPackages(Map.empty).right.get,
        scenario = true)
      val sr = ScenarioRunner(m, _ + "-XXX")
      sr.run()
      m.ctrl shouldBe Speedy.CtrlValue(SValue.SParty(Ref.Party.assertFromString("foo-bar-XXX")))
    }
  }

}
