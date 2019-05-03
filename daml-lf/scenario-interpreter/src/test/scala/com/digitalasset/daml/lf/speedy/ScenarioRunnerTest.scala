// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.lfpackage.Ast.ScenarioGetParty
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class ScenarioRunnerTest extends AsyncWordSpec with Matchers with ScalaFutures {

  "ScenarioRunner" can {
    "mangle party names correctly" in {
      val e = Ast.EScenario(ScenarioGetParty(Ast.EPrimLit(Ast.PLText("foo-bar"))))
      val m = Speedy.Machine.fromExpr(e, PureCompiledPackages(Map.empty).right.get, true)
      val sr = ScenarioRunner(m, (s) => s + "-XXX")
      sr.run()
      m.ctrl shouldBe Speedy.CtrlValue(SValue.SParty(Ref.Party.assertFromString("foo-bar-XXX")))
    }
  }

}
