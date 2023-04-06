// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package test

import java.nio.file.{Path, Paths}
import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.lf.integrationtest.CantonFixture
import com.daml.platform.services.time.TimeProviderType
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException
import io.grpc.Status.Code
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.language.implicitConversions

class ConfigSpec
    extends AsyncWordSpec
    with Matchers
    with CantonFixture
    with SuiteResourceManagementAroundAll {

  override protected def authSecret: Option[String] = None
  override protected def darFiles: List[Path] = List.empty
  override protected def devMode: Boolean = true
  override protected def nParticipants: Int = 1
  override protected def timeProviderType: TimeProviderType = TimeProviderType.Static
  override protected def tlsEnable: Boolean = false
  override protected def applicationId: ApplicationId = ApplicationId("myappid")

  private implicit def toParty(s: String): Party =
    Party(s)
  private implicit def toRefParty(s: String): Ref.Party =
    Ref.Party.assertFromString(s)
  private implicit def toUserId(s: String): UserId =
    UserId.assertFromString(s)

  private def randomUserId(): String = UUID.randomUUID().toString

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
    // FIXME: fails
    "succeed for user with primary party & actAs and readAs claims" in {
      for {
        client <- defaultLedgerClient()
        userId = randomUserId()
        _ <- client.partyManagementClient.allocateParty(hint = Some("primary"), None, None)
        _ <- client.partyManagementClient.allocateParty(hint = Some("alice"), None, None)
        _ <- client.partyManagementClient.allocateParty(hint = Some("bob"), None, None)
        _ <- client.userManagementClient.createUser(
          User(userId, Some("primary"), metadata = ObjectMeta.empty),
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
        client <- defaultLedgerClient()
        userId = randomUserId()
        ex <- recoverToExceptionIf[StatusRuntimeException](
          UserSpecification(userId).resolveClaims(client)
        )
      } yield ex.getStatus.getCode shouldBe Code.NOT_FOUND
    }
    "fail for user with no primary party" in {
      for {
        client <- defaultLedgerClient()
        userId = randomUserId()
        _ <- client.userManagementClient.createUser(
          User(userId, None, metadata = ObjectMeta.empty),
          Seq.empty,
        )
        ex <- recoverToExceptionIf[IllegalArgumentException](
          UserSpecification(userId).resolveClaims(client)
        )
      } yield ex.getMessage should include("has no primary party")
    }
    "fail for user with no actAs claims for primary party" in {
      for {
        client <- defaultLedgerClient()
        userId = randomUserId()
        _ <- client.userManagementClient.createUser(
          User(userId, Some("primary"), isDeactivated = false, ObjectMeta.empty),
          Seq.empty,
        )
        ex <- recoverToExceptionIf[IllegalArgumentException](
          UserSpecification(userId).resolveClaims(client)
        )
      } yield ex.getMessage should include("no actAs claims")
    }
    // FIXME: fails
    "succeed for user after primaryParty update" in {
      for {
        client <- defaultLedgerClient()
        userId = randomUserId()
        _ <- client.partyManagementClient.allocateParty(hint = Some("original"), None, None)
        _ <- client.partyManagementClient.allocateParty(hint = Some("updated"), None, None)
        _ <- client.partyManagementClient.allocateParty(hint = Some("other"), None, None)
        _ <- client.userManagementClient.createUser(
          User(userId, Some("original"), metadata = ObjectMeta.empty),
          Seq(
            UserRight.CanActAs("original"),
            UserRight.CanActAs("updated"),
            UserRight.CanReadAs("other"),
          ),
        )
        _ <- client.userManagementClient.updateUser(
          User(userId, Some("updated"), metadata = ObjectMeta.empty),
          Some(FieldMask(Seq("primary_party"))),
          None,
        )

        r <- UserSpecification(userId).resolveClaims(client)
      } yield r shouldBe TriggerParties("updated", Set("other", "original"))
    }
  }
}
