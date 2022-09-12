// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.ledgerinteraction.ide

import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.lf.data.Ref.{Party, UserId}
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
      mgmt.createUser(newUser("user1"), Set.empty) shouldBe Some(())
      mgmt.createUser(newUser("user2"), Set.empty) shouldBe Some(())
    }

    "disallow re-creating an existing user" in {
      val mgmt = new UserManagementStore()
      val user = newUser("user1")
      mgmt.createUser(user, Set.empty) shouldBe Some(())
      mgmt.createUser(user, Set.empty) shouldBe None
    }

    "find a freshly created user" in {
      val mgmt = new UserManagementStore()
      val user = newUser("user1")
      mgmt.createUser(user, Set.empty) shouldBe Some(())
      mgmt.getUser("user1") shouldBe Some(user)
    }

    "not find a non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.getUser("user1") shouldBe None
    }
    "not find a deleted user" in {
      val mgmt = new UserManagementStore()
      val user = newUser("user1")
      mgmt.createUser(user, Set.empty) shouldBe Some(())
      mgmt.getUser("user1") shouldBe Some(user)
      mgmt.deleteUser("user1") shouldBe Some(())
      mgmt.getUser("user1") shouldBe None
    }
    "allow recreating a deleted user" in {
      val mgmt = new UserManagementStore()
      val user = newUser("user1")
      mgmt.createUser(user, Set.empty) shouldBe Some(())
      mgmt.deleteUser(user.id) shouldBe Some(())
      mgmt.createUser(user, Set.empty) shouldBe Some(())
    }
    "fail to delete a non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.deleteUser("user1") shouldBe None
    }
    "list created users" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(newUser("user1"), Set.empty) shouldBe Some(())
      mgmt.createUser(newUser("user2"), Set.empty) shouldBe Some(())
      mgmt.listUsers() shouldBe Seq(newUser("user1"), newUser("user2"))
    }
    "not list deleted users" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(newUser("user1"), Set.empty) shouldBe Some(())
      mgmt.createUser(newUser("user2"), Set.empty) shouldBe Some(())
      mgmt.listUsers() shouldBe Seq(newUser("user1"), newUser("user2"))
      mgmt.deleteUser("user1") shouldBe Some(())
      mgmt.listUsers() shouldBe Seq(newUser("user2"))
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
      mgmt.createUser(newUser("user1"), Set.empty) shouldBe Some(())
      mgmt.listUserRights("user1") shouldBe Some(Set.empty)
      mgmt.createUser(
        newUser("user2"),
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
      ) shouldBe Some(())
      mgmt.listUserRights("user2") shouldBe Some(
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
      )
    }
    "listUserRights should fail on non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.listUserRights("user1") shouldBe None
    }
    "grantUserRights should add new rights" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(newUser("user1"), Set.empty) shouldBe Some(())
      mgmt.grantRights("user1", Set(ParticipantAdmin)) shouldBe Some(Set(ParticipantAdmin))
      mgmt.grantRights("user1", Set(ParticipantAdmin)) shouldBe Some(Set.empty)
      mgmt.grantRights("user1", Set(CanActAs("party1"), CanReadAs("party2"))) shouldBe Some(
        Set(CanActAs("party1"), CanReadAs("party2"))
      )
      mgmt.listUserRights("user1") shouldBe Some(
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
      )
    }
    "grantRights should fail on non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.grantRights("user1", Set.empty) shouldBe None
    }
    "revokeRights should revoke rights" in {
      val mgmt = new UserManagementStore()
      mgmt.createUser(
        newUser("user1"),
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
      ) shouldBe Some(())
      mgmt.listUserRights("user1") shouldBe Some(
        Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
      )
      mgmt.revokeRights("user1", Set(ParticipantAdmin)) shouldBe Some(Set(ParticipantAdmin))
      mgmt.revokeRights("user1", Set(ParticipantAdmin)) shouldBe Some(Set.empty)
      mgmt.listUserRights("user1") shouldBe Some(Set(CanActAs("party1"), CanReadAs("party2")))
      mgmt.revokeRights("user1", Set(CanActAs("party1"), CanReadAs("party2"))) shouldBe Some(
        Set(CanActAs("party1"), CanReadAs("party2"))
      )
      mgmt.listUserRights("user1") shouldBe Some(Set.empty)
    }
    "revokeRights should fail on non-existent user" in {
      val mgmt = new UserManagementStore()
      mgmt.revokeRights("user1", Set.empty) shouldBe None
    }
  }

  private def newUser(id: String): User =
    User(id = id, None, false, ObjectMeta.empty)
}
