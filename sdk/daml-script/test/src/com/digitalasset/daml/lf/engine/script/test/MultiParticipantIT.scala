// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.speedy.SValue._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.Paths
import scala.util.{Failure, Success}

class MultiParticipantIT extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {

  final override protected lazy val nParticipants = 2
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  protected override val cantonFixtureDebugMode: CantonFixtureDebugMode =
    CantonFixtureDebugKeepTmpFiles

  final override protected lazy val darFiles = List(
    rlocation(Paths.get(s"daml-script/test/template.dar")),
    rlocation(Paths.get(s"daml-script/test/retrointerface.dar")),
  )

  "Multi-participant Daml Script" can {
    "multiTest" should {
      "return 42" in {
        for {
          clients <- scriptClients()
          r <- run(clients, QualifiedName.assertFromString("MultiTest:multiTest"), dar = dar)
        } yield assert(r == SInt64(42))
      }
    }

    "partyIdHintTest" should {
      "respect party id hints" in {
        for {
          clients <- scriptClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("MultiTest:partyIdHintTest"),
            dar = dar,
          )
        } yield {
          vals should have size 2
          inside(vals.get(0)) { case SParty(p) =>
            p should startWith("alice::")
          }
          inside(vals.get(1)) { case SParty(p) =>
            p should startWith("bob::")
          }
        }
      }
    }

    "listKnownPartiesTest" should {
      "list parties on both participants" in {
        for {
          clients <- scriptClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("MultiTest:listKnownPartiesTest"),
            dar = dar,
          )
        } yield {
          assert(vals.size == 2)
          val first = SList(
            FrontStack(
              tuple(SOptional(None), SBool(false)),
              tuple(SOptional(Some(SText("p1"))), SBool(true)),
            )
          )
          assert(vals.get(0) == first)
          val second = SList(
            FrontStack(
              tuple(SOptional(None), SBool(false)),
              tuple(SOptional(Some(SText("p2"))), SBool(true)),
            )
          )
          assert(vals.get(1) == second)
        }
      }
    }

    "explicit disclosure" should {
      "works across participants" in {
        for {
          clients <- scriptClients()
          r <- run(clients, QualifiedName.assertFromString("MultiTest:disclosuresTest"), dar = dar)
        } yield assert(r == SText("my secret"))
      }
      "can be called by key" in {
        for {
          clients <- scriptClients()
          r <- run(
            clients,
            QualifiedName.assertFromString("MultiTest:disclosuresByKeyTest"),
            dar = dar,
          )
        } yield assert(r == SText("my secret"))
      }
      "does not fail during submission if inactive" in {
        for {
          clients <- scriptClients()
          error <-
            run(
              clients,
              QualifiedName.assertFromString(
                "MultiTest:inactiveDisclosureDoesNotFailDuringSubmission"
              ),
              dar = dar,
            )
              .transform {
                case Success(_) => fail("unexpected success")
                case Failure(exception) => Success(exception)
              }
        } yield error.getMessage should include regex """Unhandled Daml exception\: DA\.Exception\.GeneralError\:GeneralError\@[a-f0-9]{8}\{ message \= \"Here\" \}"""
      }
    }

    "exercise retroactive instance" in {
      // Regression test for https://github.com/DACH-NY/canton/issues/20645
      // To reproduce the bug in 2.8.10 or 2.9.4 the package retrointerface.dar
      // should not be loaded in the participant1 before starting the test.
      // Unfortunately there is n simple way to ensure that, so here are some guardrail:
      // - do not write in this file other tests that use retroactiveExercise
      // - do not load the script in the ledger, but only the template code.
      for {
        clients <- scriptClients()
        r <- run(
          clients,
          QualifiedName.assertFromString("MultiTest:retroactiveExercise"),
          dar = dar,
        )
      } yield assert(r == SInt64(43))
    }
  }
}
