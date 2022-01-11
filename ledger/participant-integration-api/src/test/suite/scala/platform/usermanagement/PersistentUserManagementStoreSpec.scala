// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.UserRight.{CanActAs, CanReadAs, ParticipantAdmin}
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{UserExists, UserNotFound}
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, UserId}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.{StorageBackendProviderPostgres, StorageBackendSpec}
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.language.implicitConversions

// TODO participant user management: Delete when certain test tool integration tests are ready to take over
class PersistentUserManagementStoreSpec
    extends AsyncFreeSpec
    with StorageBackendProviderPostgres
    with TestResourceContext
    with Matchers
    with StorageBackendSpec {

  implicit val lc: LoggingContext = LoggingContext.ForTesting

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)

  private implicit def toUserId(s: String): UserId =
    UserId.assertFromString(s)

  "do it" in {

    testIt { tested: PersistentUserManagementStore =>
      val user = User(
        id = Ref.UserId.assertFromString("user_id_123"),
        primaryParty = Some(Ref.Party.assertFromString("primary_party_123")),
      )
      val rights: Set[UserRight] = Seq(
        ParticipantAdmin,
        CanActAs(Ref.Party.assertFromString("party_act_as_1")),
        CanActAs(Ref.Party.assertFromString("party_act_as_2")),
        CanReadAs(Ref.Party.assertFromString("party_read_as_1")),
      ).toSet

      for {
        _ <- tested.createUser(user, rights)
        createdUser <- tested.getUser(user.id)
        nonExistentUser <- tested.getUser(Ref.UserId.assertFromString("user_id_123_non_existent"))
      } yield {
        createdUser shouldBe Right(user)
        nonExistentUser shouldBe Left(
          UserManagementStore.UserNotFound(Ref.UserId.assertFromString("user_id_123_non_existent"))
        )
      }

    }

  }

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

  def testIt(f: PersistentUserManagementStore => Future[Assertion]): Future[Assertion] = {
    val metrics = new Metrics(new MetricRegistry)
    val tested = new PersistentUserManagementStore(
      dbDispatcher = getDbDispatcher,
      metrics = metrics,
    )
    f(tested)

  }

}
