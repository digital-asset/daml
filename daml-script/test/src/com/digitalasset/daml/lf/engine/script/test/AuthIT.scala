// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.value.Value
import com.daml.lf.speedy.SValue._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import scala.concurrent.Future
import scala.util.{Failure, Success}

final class AuthIT
    extends AsyncWordSpec
    with CantonFixture
    with Matchers
    with SuiteResourceManagementAroundAll {
  import AbstractScriptTest._

  // TODO: https://github.com/digital-asset/daml/issues/16539
  //  The test is broken, the commented `transform` called in the for construct
  "Daml Script against authorized ledger" can {
    "auth" should {
      "create and accept Proposal" in {
        for {
          nonAuthenticatedClients <- participantClients()
          grpcClient = nonAuthenticatedClients.default_participant.get
          parties <- Future.sequence(
            List.fill(2)(grpcClient.allocateParty("", ""))
          )
          value = Some(
            Value.ValueRecord(
              None,
              ImmArray(
                None -> Value.ValueParty(Party.assertFromString(parties.head)),
                None -> Value.ValueParty(Party.assertFromString(parties.tail.head)),
              ),
            )
          )
          _ <- run(
            nonAuthenticatedClients,
            QualifiedName.assertFromString("ScriptTest:auth"),
            inputValue = value,
            dar = stableDar,
          )
          //  .transform {
          //    case Failure(_) => Success(())
          //    case Success(_) => Failure(new Exception("unexpected success"))
          //  }
          authenticatedClients <- participantClients(Some(parties))
          r <- run(
            authenticatedClients,
            QualifiedName.assertFromString("ScriptTest:auth"),
            inputValue = value,
            dar = stableDar,
          )
        } yield assert(
          r == SUnit
        ) // Boring assertion, we just want to see that we do not get an authorization error
      }
    }
  }

  override protected val timeMode: ScriptTimeMode = ScriptTimeMode.WallClock
  override protected def darFiles: List[File] = List(stableDarPath)
  override protected def nParticipants: Int = 1
  override protected def devMode: Boolean = false
  override protected def tlsEnable: Boolean = false
}
