// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.ledgerinteraction.ide

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref.{Party, UserId}
import com.daml.lf.scenario.Error.UserManagementError._
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec
import scala.language.implicitConversions

final class InMemoryUserManagementStoreSpec extends AnyFreeSpec with Matchers {

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)
  private implicit def toUserId(s: String): UserId =
    UserId.assertFromString(s)

  // tests for
  //   createUser
  //   deleteUser
  //   getUser
  //   createUser
  //   listUsers
  "in-memory user management" - {
    "allow creating a fresh user" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(User("user1", None), Set.empty) shouldBe Right(())
      mgmt.createUser(User("user2", None), Set.empty) shouldBe Right(())
    }

    "disallow re-creating an existing user" in {
      val mgmt = new UserManagementStore()
      val user = User("user1", None)
      mgmt.createUser(user, Set.empty) shouldBe Right(())
      mgmt.createUser(user, Set.empty) shouldBe Left(UserExists("user1"))
    }

    "find a freshly created user" in {
      val mgmt = new UserManagementStore()
      val user = User("user1", None)
      mgmt.createUser(user, Set.empty) shouldBe Right(())
      mgmt.getUser("user1") shouldBe Right(user)
    }

    "not find a non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.getUser("user1") shouldBe Left(UserNotFound("user1"))
    }
    "not find a deleted user" in {
      val mgmt = new UserManagementStore()
      val user = User("user1", None)
      mgmt.createUser(user, Set.empty) shouldBe Right(())
      mgmt.getUser("user1") shouldBe Right(user)
      mgmt.deleteUser("user1") shouldBe Right(())
      mgmt.getUser("user1") shouldBe Left(UserNotFound("user1"))
    }
    "allow recreating a deleted user" in {
      val mgmt = new UserManagementStore()
      val user = User("user1", None)
      mgmt.createUser(user, Set.empty) shouldBe Right(())
      mgmt.deleteUser(user.id) shouldBe Right(())
      mgmt.createUser(user, Set.empty) shouldBe Right(())
    }
    "fail to delete a non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.deleteUser("user1") shouldBe Left(UserNotFound("user1"))
    }
    "list created users" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(User("user1", None), Set.empty) shouldBe Right(())
      mgmt.createUser(User("user2", None), Set.empty) shouldBe Right(())
      mgmt.listUsers() shouldBe Right(Seq(User("user1", None), User("user2", None)))
    }
    "not list deleted users" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(User("user1", None), Set.empty) shouldBe Right(())
      mgmt.createUser(User("user2", None), Set.empty) shouldBe Right(())
      mgmt.listUsers() shouldBe Right(Seq(User("user1", None), User("user2", None)))
      mgmt.deleteUser("user1") shouldBe Right(())
      mgmt.listUsers() shouldBe Right(Seq(User("user2", None)))
    }
  }

  // tests for:
  //    listUserRights
  //    revokeRights
  //    grantRights
  "in-memory user rights management" - {
    import UserRight._
    "listUserRights should find the rights of a freshly created user" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(User("user1", None), Set.empty) shouldBe Right(())
      mgmt.listUserRights("user1") shouldBe Right(Set.empty)
      mgmt.createUser(
        User("user2", None),
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
      ) shouldBe Right(())
      mgmt.listUserRights("user2") shouldBe Right(
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
      )
    }
    "listUserRights should fail on non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.listUserRights("user1") shouldBe Left(UserNotFound("user1"))
    }
    "grantUserRights should add new rights" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(User("user1", None), Set.empty) shouldBe Right(())
      mgmt.grantRights("user1", Set(ParticipantAdmin)) shouldBe Right(Set(ParticipantAdmin))
      mgmt.grantRights("user1", Set(ParticipantAdmin)) shouldBe Right(Set.empty)
      mgmt.grantRights("user1", Set(CanActAs("party1"), CanReadAs("party2"))) shouldBe Right(
        Set(CanActAs("party1"), CanReadAs("party2"))
      )
      mgmt.listUserRights("user1") shouldBe Right(
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
      )
    }
    "grantRights should fail on non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.grantRights("user1", Set.empty) shouldBe Left(UserNotFound("user1"))
    }
    "revokeRights should revoke rights" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(
        User("user1", None),
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
      ) shouldBe Right(())
      mgmt.listUserRights("user1") shouldBe Right(
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
      )
      mgmt.revokeRights("user1", Set(ParticipantAdmin)) shouldBe Right(Set(ParticipantAdmin))
      mgmt.revokeRights("user1", Set(ParticipantAdmin)) shouldBe Right(Set.empty)
      mgmt.listUserRights("user1") shouldBe Right(Set(CanActAs("party1"), CanReadAs("party2")))
      mgmt.revokeRights("user1", Set(CanActAs("party1"), CanReadAs("party2"))) shouldBe Right(
        Set(CanActAs("party1"), CanReadAs("party2"))
      )
      mgmt.listUserRights("user1") shouldBe Right(Set.empty)
    }
    "revokeRights should fail on non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.revokeRights("user1", Set.empty) shouldBe Left(UserNotFound("user1"))
    }
  }
}
