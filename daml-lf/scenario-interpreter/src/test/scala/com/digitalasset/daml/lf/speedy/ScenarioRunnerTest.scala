// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast.ScenarioGetParty
import com.daml.lf.transaction.TransactionVersions
import com.daml.lf.value.ValueVersions
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class ScenarioRunnerTest extends AsyncWordSpec with Matchers with ScalaFutures {

  "ScenarioRunner" can {
    "mangle party names correctly" in {
      val compiledPackages = PureCompiledPackages(Map.empty).right.get
      val e = Ast.EScenario(ScenarioGetParty(Ast.EPrimLit(Ast.PLText(("foo-bar")))))
      val txSeed = crypto.Hash.hashPrivateKey("ScenarioRunnerTest")
      val m = Speedy.Machine.fromScenarioExpr(
        compiledPackages,
        txSeed,
        e,
        ValueVersions.SupportedVersions,
        TransactionVersions.SupportedVersions,
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
