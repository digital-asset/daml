// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.digitalasset.canton.ledger.api
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.daml.integrationtest._
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.util.{Failure, Success}

class AuthITV2 extends AuthIT(LanguageMajorVersion.V2)

class AuthIT(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers {

  final override protected lazy val authSecret = Some("secret")
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  "Daml Script against authorized ledger" can {
    "auth" should {
      "create and accept Proposal" in {
        for {
          adminClient <- defaultLedgerClient(config.adminToken)
          userId = Ref.UserId.assertFromString(CantonFixture.freshUserId())
          partyDetails <- Future.sequence(
            List.fill(2)(adminClient.partyManagementClient.allocateParty(hint = None, token = None))
          )
          parties = partyDetails.map(_.party)
          user = api.User(userId, None)
          rights = parties.map(api.UserRight.CanActAs(_))
          _ <- adminClient.userManagementClient.createUser(user, rights)
          // we double check authentification is on
          wrongToken = CantonRunner.getToken(userId, Some("not secret"))
          err <- scriptClients(token = wrongToken).transform {
            case Failure(err) => Success(err)
            case Success(_) => Failure(new Exception("unexpected success"))
          }
          _ = info(s"client creation with wrong token fails with $err")
          goodToken = config.getToken(userId)
          clients <- scriptClients(token = goodToken)
          _ = info(s"client creation with valid token succeeds")
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("ScriptTest:auth"),
            inputValue = Some(
              Value.ValueRecord(
                None,
                ImmArray(
                  None -> Value.ValueParty(Ref.Party.assertFromString(parties.head)),
                  None -> Value.ValueParty(Ref.Party.assertFromString(parties.tail.head)),
                ),
              )
            ),
            dar = dar,
          )
        } yield succeed
      }
    }
  }
}
