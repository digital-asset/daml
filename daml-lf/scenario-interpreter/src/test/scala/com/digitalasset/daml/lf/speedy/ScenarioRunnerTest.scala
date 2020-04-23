// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast.ScenarioGetParty
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class ScenarioRunnerTest extends AsyncWordSpec with Matchers with ScalaFutures {

  "ScenarioRunner" can {
    "mangle party names correctly" in {
      val e = Ast.EScenario(ScenarioGetParty(Ast.EPrimLit(Ast.PLText(("foo-bar")))))
      val m = Speedy.Machine.fromExpr(
        expr = e,
        compiledPackages = PureCompiledPackages(Map.empty).right.get,
        scenario = true,
        submissionTime = Time.Timestamp.now(),
        transactionSeed = None
      )
      val sr = ScenarioRunner(m, _ + "-XXX")
      sr.run()
      m.ctrl shouldBe Speedy.CtrlValue(SValue.SParty(Ref.Party.assertFromString("foo-bar-XXX")))
    }
  }

}
