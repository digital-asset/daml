// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ledgerinteraction.GrpcLedgerClient
import com.daml.lf.speedy.SValue._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import spray.json.{JsObject, JsString}

import scala.concurrent.Future

final class AuthIT
    extends AsyncWordSpec
    with SandboxAuthParticipantFixture
    with Matchers
    with SuiteResourceManagementAroundAll {
  override def darFile = new File(rlocation("daml-script/test/script-test.dar"))
  val (dar, envIface) = readDar(darFile)
  val parties = List("Alice", "Bob")

  "Daml Script against authorized ledger" can {
    "auth" should {
      "create and accept Proposal" in {
        for {
          clients <- participantClients(parties, false)
          grpcClient <- clients.default_participant.fold(
            Future.failed[GrpcLedgerClient](
              new IllegalStateException("Missing default GrpcLedgerClient")
            )
          )(Future.successful[GrpcLedgerClient])
          _ <- Future.sequence(
            parties.map(p => grpcClient.allocateParty(p, ""))
          )
          r <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:auth"),
            inputValue =
              Some(JsObject(("_1", JsString(parties.head)), ("_2", JsString(parties.tail.head)))),
            dar = dar,
          )
        } yield assert(
          r == SUnit
        ) // Boring assertion, we just want to see that we do not get an authorization error
      }
    }
  }
}
