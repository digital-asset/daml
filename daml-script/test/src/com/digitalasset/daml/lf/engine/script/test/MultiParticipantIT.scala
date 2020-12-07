// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.speedy.SValue._
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class MultiParticipantIT
    extends AsyncWordSpec
    with MultiParticipantFixture
    with Matchers
    with SuiteResourceManagementAroundAll {
  private def darFile = new File(rlocation("daml-script/test/script-test.dar"))
  val (dar, envIface) = readDar(darFile)

  "Multi-participant DAML Script" can {
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
            dar = dar)
        } yield {
          vals should contain theSameElementsInOrderAs Seq("alice", "bob").map(p =>
            SParty(Party.assertFromString(p)))
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
            dar = dar)
        } yield {
          assert(vals.size == 2)
          val first = SList(
            FrontStack(
              tuple(SOptional(Some(SText("p1"))), SBool(true)),
              tuple(SOptional(Some(SText("p2"))), SBool(false))))
          assert(vals.get(0) == first)
          val second = SList(
            FrontStack(
              tuple(SOptional(Some(SText("p1"))), SBool(false)),
              tuple(SOptional(Some(SText("p2"))), SBool(true))))
          assert(vals.get(1) == second)
        }

      }
    }
  }
}
