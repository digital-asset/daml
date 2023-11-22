// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.ledger.api.domain
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.integrationtest._
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.util.{Failure, Success}

class AuthITV1 extends AuthIT(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
class AuthITV2 extends AuthIT(LanguageMajorVersion.V2)

class AuthIT(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers {

  final override protected lazy val authSecret = Some("secret")
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  // TODO(#17366): Delete once 2.0 is introduced and Canton supports LF v2 in non-dev mode.
  final override protected lazy val devMode = (majorLanguageVersion == LanguageMajorVersion.V2)

  "Daml Script against authorized ledger" can {
    "auth" should {
      "create and accept Proposal" in {
        for {
          adminClient <- defaultLedgerClient(config.adminToken)
          userId = Ref.UserId.assertFromString(CantonFixture.freshUserId())
          partyDetails <- Future.sequence(
            List.fill(2)(adminClient.partyManagementClient.allocateParty(None, None))
          )
          parties = partyDetails.map(_.party)
          user = domain.User(userId, None)
          rights = parties.map(domain.UserRight.CanActAs(_))
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
