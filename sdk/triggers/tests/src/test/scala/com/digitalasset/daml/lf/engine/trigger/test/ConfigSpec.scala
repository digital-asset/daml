// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package test

import java.nio.file.Paths
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException
import io.grpc.Status.Code
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.language.implicitConversions

class ConfigSpec extends AsyncWordSpec with Matchers with CantonFixture {

  private implicit def toParty(s: String): Ref.Party =
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
        config.copy(ledgerClaims = PartySpecification(TriggerParties("alice", Set.empty)))
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
        adminClient <- defaultLedgerClient(config.adminToken)
        userId = CantonFixture.freshUserId()
        primary <- adminClient.partyManagementClient.allocateParty(
          hint = Some("primary"),
          None,
          None,
        )
        alice <- adminClient.partyManagementClient.allocateParty(hint = Some("alice"), None, None)
        bob <- adminClient.partyManagementClient.allocateParty(hint = Some("bob"), None, None)
        _ <- adminClient.userManagementClient.createUser(
          User(userId, Some(primary.party), metadata = ObjectMeta.empty),
          Seq(
            UserRight.CanActAs(primary.party),
            UserRight.CanActAs(alice.party),
            UserRight.CanReadAs(bob.party),
          ),
        )
        r <- UserSpecification(userId).resolveClaims(adminClient)
      } yield r shouldBe TriggerParties(primary.party, Set(alice.party, bob.party))
    }
    "fail for non-existent user" in {
      for {
        adminClient <- defaultLedgerClient(config.adminToken)
        userId = CantonFixture.freshUserId()
        ex <- recoverToExceptionIf[StatusRuntimeException](
          UserSpecification(userId).resolveClaims(adminClient)
        )
      } yield ex.getStatus.getCode shouldBe Code.NOT_FOUND
    }
    "fail for user with no primary party" in {
      for {
        adminClient <- defaultLedgerClient(config.adminToken)
        userId = CantonFixture.freshUserId()
        _ <- adminClient.userManagementClient.createUser(
          User(userId, None, metadata = ObjectMeta.empty),
          Seq.empty,
        )
        ex <- recoverToExceptionIf[IllegalArgumentException](
          UserSpecification(userId).resolveClaims(adminClient)
        )
      } yield ex.getMessage should include("has no primary party")
    }
    "fail for user with no actAs claims for primary party" in {
      for {
        adminClient <- defaultLedgerClient(config.adminToken)
        userId = CantonFixture.freshUserId()
        _ <- adminClient.userManagementClient.createUser(
          User(userId, Some("primary"), isDeactivated = false, ObjectMeta.empty),
          Seq.empty,
        )
        ex <- recoverToExceptionIf[IllegalArgumentException](
          UserSpecification(userId).resolveClaims(adminClient)
        )
      } yield ex.getMessage should include("no actAs claims")
    }
    "succeed for user after primaryParty update" in {
      for {
        adminClient <- defaultLedgerClient(config.adminToken)
        userId = CantonFixture.freshUserId()
        original <- adminClient.partyManagementClient.allocateParty(
          hint = Some("original"),
          None,
          None,
        )
        updated <- adminClient.partyManagementClient.allocateParty(
          hint = Some("updated"),
          None,
          None,
        )
        other <- adminClient.partyManagementClient.allocateParty(hint = Some("other"), None, None)
        _ <- adminClient.userManagementClient.createUser(
          User(userId, Some(original.party), metadata = ObjectMeta.empty),
          Seq(
            UserRight.CanActAs(original.party),
            UserRight.CanActAs(updated.party),
            UserRight.CanReadAs(other.party),
          ),
        )
        _ <- adminClient.userManagementClient.updateUser(
          User(userId, Some(updated.party), metadata = ObjectMeta.empty),
          Some(FieldMask(Seq("primary_party"))),
          None,
        )

        r <- UserSpecification(userId).resolveClaims(adminClient)
      } yield r shouldBe TriggerParties(updated.party, Set(other.party, original.party))
    }
  }
}
