// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package test

import java.nio.file.Paths

import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.platform.sandbox.fixture.SandboxFixture
import io.grpc.StatusRuntimeException
import io.grpc.Status.Code
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.language.implicitConversions

class ConfigSpec
    extends AsyncWordSpec
    with Matchers
    with SandboxFixture
    with SuiteResourceManagementAroundAll {

  private val clientConfig = LedgerClientConfiguration(
    applicationId = "myappid",
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    token = None,
  )

  override protected val packageFiles = List.empty

  private implicit def toParty(s: String): Party =
    Party(s)
  private implicit def toRefParty(s: String): Ref.Party =
    Ref.Party.assertFromString(s)
  private implicit def toUserId(s: String): UserId =
    UserId.assertFromString(s)

  "CLI" should {
    val defaultArgs = Array(
      "--dar=dummy.dar",
      "--trigger-name=M:t",
      "--ledger-host=localhost",
      "--ledger-port=6865",
    )
    val config = RunnerConfig.Empty.copy(
      darPath = Paths.get("dummy.dar"),
      ledgerHost = "localhost",
      ledgerPort = 6865,
      triggerIdentifier = "M:t",
    )
    "succeed with --ledger-party" in {
      RunnerConfig.parse(defaultArgs ++ Seq("--ledger-party=alice")) shouldBe Some(
        config.copy(ledgerClaims = PartySpecification(TriggerParties(Party("alice"), Set.empty)))
      )
    }
    "succeed with --ledger-party and --ledger-readas" in {
      RunnerConfig.parse(
        defaultArgs ++ Seq("--ledger-party=alice", "--ledger-readas=bob,charlie")
      ) shouldBe Some(
        config.copy(ledgerClaims =
          PartySpecification(TriggerParties("alice", Set("bob", "charlie")))
        )
      )
    }
    "succeed with --ledger-user" in {
      RunnerConfig.parse(defaultArgs ++ Seq("--ledger-user=alice")) shouldBe Some(
        config.copy(ledgerClaims = UserSpecification("alice"))
      )
    }
    "fail with --ledger-readas but no --ledger-party" in {
      RunnerConfig.parse(defaultArgs ++ Seq("--ledger-readas=alice")) shouldBe None
    }
    "fail with --ledger-party and --ledger-user" in {
      RunnerConfig.parse(
        defaultArgs ++ Seq("--ledger-party=alice", "--ledger-user=bob")
      ) shouldBe None
    }
    "fail with --ledger-party, --ledger-readas and --ledger-user" in {
      RunnerConfig.parse(
        defaultArgs ++ Seq("--ledger-party=alice", "--ledger-readas=charlie", "--ledger-user=bob")
      ) shouldBe None
    }
  }

  "resolveClaims" should {
    "succeed for user with primary party & actAs and readAs claims" in {
      for {
        client <- LedgerClient(channel, clientConfig)
        userId = randomUserId()
        _ <- client.userManagementClient.createUser(
          User(userId, Some("primary"), isDeactivated = false, metadata = ObjectMeta.empty),
          Seq(
            UserRight.CanActAs("primary"),
            UserRight.CanActAs("alice"),
            UserRight.CanReadAs("bob"),
          ),
        )
        r <- UserSpecification(userId).resolveClaims(client)
      } yield r shouldBe TriggerParties("primary", Set("alice", "bob"))
    }
    "fail for non-existent user" in {
      for {
        client <- LedgerClient(channel, clientConfig)
        ex <- recoverToExceptionIf[StatusRuntimeException](
          UserSpecification(randomUserId()).resolveClaims(client)
        )
      } yield ex.getStatus.getCode shouldBe Code.NOT_FOUND
    }
    "fail for user with no primary party" in {
      for {
        client <- LedgerClient(channel, clientConfig)
        userId = randomUserId()
        _ <- client.userManagementClient.createUser(
          User(userId, None, false, ObjectMeta.empty),
          Seq.empty,
        )
        ex <- recoverToExceptionIf[IllegalArgumentException](
          UserSpecification(userId).resolveClaims(client)
        )
      } yield ex.getMessage should include("has no primary party")
    }
    "fail for user with no actAs claims for primary party" in {
      for {
        client <- LedgerClient(channel, clientConfig)
        userId = randomUserId()
        _ <- client.userManagementClient.createUser(
          User(userId, Some("primary"), false, ObjectMeta.empty),
          Seq.empty,
        )
        ex <- recoverToExceptionIf[IllegalArgumentException](
          UserSpecification(userId).resolveClaims(client)
        )
      } yield ex.getMessage should include("no actAs claims")
    }
  }
}
