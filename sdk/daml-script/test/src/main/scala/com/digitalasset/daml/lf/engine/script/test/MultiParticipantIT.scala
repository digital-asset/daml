// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.speedy.SValue._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.{Failure, Success}

class MultiParticipantITV2 extends MultiParticipantIT(LanguageMajorVersion.V2)

class MultiParticipantIT(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractScriptTest
    with Inside
    with Matchers {

  final override protected lazy val nParticipants = 2
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

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
          inside(vals(0)) { case SParty(p) =>
            p should startWith("alice::")
          }
          inside(vals(1)) { case SParty(p) =>
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
              tuple(SText("p1"), SBool(true)),
              tuple(SText("p2"), SBool(false)),
            )
          )
          assert(vals(0) == first)
          val second = SList(
            FrontStack(
              tuple(SText("p1"), SBool(false)),
              tuple(SText("p2"), SBool(true)),
            )
          )
          assert(vals(1) == second)
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
        } yield error.getMessage should include(
          "User failure: UNHANDLED_EXCEPTION/DA.Exception.GeneralError:GeneralError (error category 9): Here"
        )
      }
    }
  }
}
