// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import ch.qos.logback.classic.Level
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.UserRight.{CanActAs, CanReadAs, ParticipantAdmin}
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryUserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{UserExists, UserNotFound}
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, UserId}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.{StorageBackendProviderPostgres, StorageBackendSpec_forStore}
import com.daml.platform.testing.{LogCollector, LogCollectorAssertions}
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.language.implicitConversions

class UserManagementStoreSpec_PersistentUserManagementStore extends UserManagementStoreSpecBase {

  override def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = {
    val metrics = new Metrics(new MetricRegistry)
    val tested = new PersistentUserManagementStore(
      dbDispatcher = getDbDispatcher,
      metrics = metrics,
    )
    f(tested)
  }

  "log on user creation, deletion, rights granting and revocation" in {
    val user = User(
      id = Ref.UserId.assertFromString("user_id_123"),
      primaryParty = Some(Ref.Party.assertFromString("primary_party_123")),
    )
    val rights: Set[UserRight] = Seq(
      ParticipantAdmin,
      CanActAs(Ref.Party.assertFromString("party_act_as_1")),
      CanActAs(Ref.Party.assertFromString("party_act_as_2")),
      CanReadAs(Ref.Party.assertFromString("party_read_as_1")),
      CanReadAs(Ref.Party.assertFromString("party_read_as_2")),
      CanReadAs(Ref.Party.assertFromString("party_read_as_3")),
    ).toSet
    val right1 = CanReadAs(Ref.Party.assertFromString("party_read_as_4"))
    val right2 = CanReadAs(Ref.Party.assertFromString("party_read_as_5"))
    val rights2: Set[UserRight] = Set(right1, right2)

    testIt { tested =>
      def assertOneLogThenClear: LogCollector.Entry = {
        val logs =
          LogCollector.readAsEntries[UserManagementStoreSpecBase, PersistentUserManagementStore]
        LogCollector.clear[UserManagementStoreSpecBase]
        logs.size shouldBe 1
        logs.head
      }
      LogCollector.clear[UserManagementStoreSpecBase]
      for {
        _ <- tested.createUser(user, rights)
        createUserLog = assertOneLogThenClear

        _ <- tested.grantRights(user.id, rights2)
        grantRightsLog = assertOneLogThenClear
        _ <- tested.revokeRights(user.id, Set(right1, right2))
        revokeRightLog = assertOneLogThenClear
        _ <- tested.deleteUser(user.id)
        deleteUserLog = assertOneLogThenClear
      } yield {
        assertLogEntry(
          createUserLog,
          expectedLogLevel = Level.INFO,
          expectedMsg =
            s"Created new user: $user with 6 rights: ${rights.take(5).mkString(", ")}, ...",
        )
        assertLogEntry(
          grantRightsLog,
          expectedLogLevel = Level.INFO,
          expectedMsg = s"Granted 2 user rights to user ${user.id}: ${rights2.mkString(", ")}",
        )
        assertLogEntry(
          revokeRightLog,
          expectedLogLevel = Level.INFO,
          expectedMsg = s"Revoked 2 user rights from user ${user.id}: ${rights2.mkString(", ")}",
        )
        assertLogEntry(
          deleteUserLog,
          expectedLogLevel = Level.INFO,
          expectedMsg = "Deleted user with id: user_id_123",
        )
      }
    }
  }

}

class UserManagementStoreSpec_InMemoryUserManagementStore extends UserManagementStoreSpecBase {

  override def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = {
    val tested = new InMemoryUserManagementStore(
      createAdmin = false
    )
    f(tested)
  }

}

class UserManagementStoreSpec_CachedUserManagementStore extends UserManagementStoreSpecBase {

  override def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = {
    val metrics = new Metrics(new MetricRegistry)
    val delegate = new PersistentUserManagementStore(
      dbDispatcher = getDbDispatcher,
      metrics = metrics,
    )
    val tested = new CachedUserManagementStore(
      delegate,
      expiryAfterWriteInSeconds = 10,
      maximumCacheSize = 100,
      new Metrics(new MetricRegistry)
    )
    f(tested)
  }

}

trait UserManagementStoreSpecBase
    extends AsyncFreeSpec
    with StorageBackendProviderPostgres
    with TestResourceContext
    with Matchers
    // TODO pbatko: This is not storage backend
    with StorageBackendSpec_forStore
    with LogCollectorAssertions {

  implicit val lc: LoggingContext = LoggingContext.ForTesting

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)

  private implicit def toUserId(s: String): UserId =
    UserId.assertFromString(s)

  def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion]

  "user management" - {
    "allow creating a fresh user" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(User(s"user1", None), Set.empty)
          res2 <- tested.createUser(User("user2", None), Set.empty)
        } yield {
          res1 shouldBe Right(())
          res2 shouldBe Right(())
        }
      }
    }

    "disallow re-creating an existing user" in {
      testIt { tested =>
        val user = User("user1", None)
        for {
          res1 <- tested.createUser(user, Set.empty)
          res2 <- tested.createUser(user, Set.empty)
        } yield {
          res1 shouldBe Right(())
          res2 shouldBe Left(UserExists(user.id))
        }
      }
    }

    "find a freshly created user" in {
      testIt { tested =>
        val user = User("user1", None)
        for {
          res1 <- tested.createUser(user, Set.empty)
          user1 <- tested.getUser(user.id)
        } yield {
          res1 shouldBe Right(())
          user1 shouldBe Right(user)
        }
      }
    }

    "not find a non-existent user" in {
      testIt { tested =>
        val userId: Ref.UserId = "user1"
        for {
          user1 <- tested.getUser(userId)
        } yield {
          user1 shouldBe Left(UserNotFound(userId))
        }
      }
    }
    "not find a deleted user" in {
      testIt { tested =>
        val user = User("user1", None)
        for {
          res1 <- tested.createUser(user, Set.empty)
          user1 <- tested.getUser("user1")
          res2 <- tested.deleteUser("user1")
          user2 <- tested.getUser("user1")
        } yield {
          res1 shouldBe Right(())
          user1 shouldBe Right(user)
          res2 shouldBe Right(())
          user2 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
    "allow recreating a deleted user" in {
      testIt { tested =>
        val user = User("user1", None)
        for {
          res1 <- tested.createUser(user, Set.empty)
          res2 <- tested.deleteUser(user.id)
          res3 <- tested.createUser(user, Set.empty)
        } yield {
          res1 shouldBe Right(())
          res2 shouldBe Right(())
          res3 shouldBe Right(())
        }

      }
    }
    "fail to delete a non-existent user" in {
      testIt { tested =>
        for {
          res1 <- tested.deleteUser("user1")
        } yield {
          res1 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
    "list created users" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(User("user1", None), Set.empty)
          res2 <- tested.createUser(User("user2", None), Set.empty)
          users1 <- tested.listUsers()

        } yield {
          res1 shouldBe Right(())
          res2 shouldBe Right(())
          users1 shouldBe Right(Seq(User("user1", None), User("user2", None)))

        }
      }
    }
    "not list deleted users" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(User("user1", None), Set.empty)
          res2 <- tested.createUser(User("user2", None), Set.empty)
          users1 <- tested.listUsers()
          res3 <- tested.deleteUser("user1")
          users2 <- tested.listUsers()
        } yield {
          res1 shouldBe Right(())
          res2 shouldBe Right(())
          users1 shouldBe Right(Seq(User("user1", None), User("user2", None)))
          res3 shouldBe Right(())
          users2 shouldBe Right(Seq(User("user2", None)))

        }
      }
    }
  }

  "user rights management" - {
    import UserRight._
    "listUserRights should find the rights of a freshly created user" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(User("user1", None), Set.empty)
          rights1 <- tested.listUserRights("user1")
          user2 <- tested.createUser(
            User("user2", None),
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
          )
          rights2 <- tested.listUserRights("user2")
        } yield {
          res1 shouldBe Right(())
          rights1 shouldBe Right(Set.empty)
          user2 shouldBe Right(())
          rights2 shouldBe Right(
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
          )
        }
      }
    }
    "listUserRights should fail on non-existent user" in {
      testIt { tested =>
        for {
          rights1 <- tested.listUserRights("user1")
        } yield {
          rights1 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
    "grantUserRights should add new rights" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(User("user1", None), Set.empty)
          rights1 <- tested.grantRights("user1", Set(ParticipantAdmin))
          rights2 <- tested.grantRights("user1", Set(ParticipantAdmin))
          rights3 <- tested.grantRights("user1", Set(CanActAs("party1"), CanReadAs("party2")))
          rights4 <- tested.listUserRights("user1")
        } yield {
          res1 shouldBe Right(())
          rights1 shouldBe Right(Set(ParticipantAdmin))
          rights2 shouldBe Right(Set.empty)
          rights3 shouldBe Right(
            Set(CanActAs("party1"), CanReadAs("party2"))
          )
          rights4 shouldBe Right(
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
          )
        }
      }
    }
    "grantRights should fail on non-existent user" in {
      testIt { tested =>
        for {
          rights1 <- tested.grantRights("user1", Set.empty)
        } yield {
          rights1 shouldBe Left(UserNotFound("user1"))
        }

      }
    }
    "revokeRights should revoke rights" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(
            User("user1", None),
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
          )
          rights1 <- tested.listUserRights("user1")
          rights2 <- tested.revokeRights("user1", Set(ParticipantAdmin))
          rights3 <- tested.revokeRights("user1", Set(ParticipantAdmin))
          rights4 <- tested.listUserRights("user1")
          rights5 <- tested.revokeRights("user1", Set(CanActAs("party1"), CanReadAs("party2")))
          rights6 <- tested.listUserRights("user1")
        } yield {
          res1 shouldBe Right(())
          rights1 shouldBe Right(
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2"))
          )
          rights2 shouldBe Right(Set(ParticipantAdmin))
          rights3 shouldBe Right(Set.empty)
          rights4 shouldBe Right(Set(CanActAs("party1"), CanReadAs("party2")))
          rights5 shouldBe Right(
            Set(CanActAs("party1"), CanReadAs("party2"))
          )
          rights6 shouldBe Right(Set.empty)
        }
      }
    }
    "revokeRights should fail on non-existent user" in {
      testIt { tested =>
        for {
          rights1 <- tested.revokeRights("user1", Set.empty)
        } yield {
          rights1 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
  }

}
