// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.speedy.SValue._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File

final class MultiParticipantIT
    extends AsyncWordSpec
    with Inside
    with CantonFixture
    with Matchers
    with SuiteResourceManagementAroundAll {
  import AbstractScriptTest._

  private def darFile = new File(rlocation("daml-script/test/script-test.dar"))
  val dar = AbstractScriptTest.readDar(darFile)

  protected override def darFiles = List(darFile)
  protected override val devMode = true
  protected override val nParticipants = 2
  protected override def timeMode = ScriptTimeMode.WallClock

  "Multi-participant Daml Script" can {
    "multiTest" should {
      "return 42" in {
        for {
          clients <- participantClients()
          r <- run(clients, QualifiedName.assertFromString("MultiTest:multiTest"), dar = dar)
        } yield assert(r == SInt64(42))
      }
    }
    "partyIdHintTest" should {
      "respect party id hints" in {
        for {
          clients <- participantClients()
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
          clients <- participantClients()
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
  }
}
