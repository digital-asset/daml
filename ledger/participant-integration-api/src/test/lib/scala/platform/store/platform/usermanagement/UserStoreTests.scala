// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.platform.usermanagement

import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.ledger.participant.state.index.v2.AnnotationsUpdate.{Merge, Replace}
import com.daml.ledger.participant.state.index.v2.{
  AnnotationsUpdate,
  ObjectMetaUpdate,
  UserManagementStore,
  UserUpdate,
}
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{
  MaxAnnotationsSizeExceeded,
  UserExists,
  UserNotFound,
  UsersPage,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, UserId}
import com.daml.logging.LoggingContext
import org.scalatest.freespec.AsyncFreeSpec

import scala.language.implicitConversions

/** Common tests for implementations of [[UserManagementStore]]
  */
trait UserStoreTests extends UserStoreSpecBase { self: AsyncFreeSpec =>

  implicit val lc: LoggingContext = LoggingContext.ForTesting

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)

  private implicit def toUserId(s: String): UserId =
    UserId.assertFromString(s)

  private val userId1 = "user1"

  def newUser(name: String = userId1, annotations: Map[String, String] = Map.empty): User = User(
    id = name,
    primaryParty = None,
    isDeactivated = false,
    metadata = ObjectMeta(
      resourceVersionO = None,
      annotations = annotations,
    ),
  )

  def createdUser(
      name: String = userId1,
      resourceVersion: Long = 0,
      annotations: Map[String, String] = Map.empty,
  ): User =
    User(
      id = name,
      primaryParty = None,
      isDeactivated = false,
      metadata = ObjectMeta(
        resourceVersionO = Some(resourceVersion),
        annotations = annotations,
      ),
    )

  // TODO um-for-hub: Consider defining a method like this directly on UserUpdate
  def makeUserUpdate(
      id: String = userId1,
      primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
      isDeactivatedUpdateO: Option[Boolean] = None,
      annotationsUpdateO: Option[AnnotationsUpdate] = None,
  ): UserUpdate = UserUpdate(
    id = id,
    primaryPartyUpdateO = primaryPartyUpdateO,
    isDeactivatedUpdateO = isDeactivatedUpdateO,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
  )

  def resetResourceVersion(
      user: User
  ): User =
    user.copy(metadata = user.metadata.copy(resourceVersionO = None))

  "user management" - {

    "allow creating a fresh user" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(newUser(s"user1"), Set.empty)
          res2 <- tested.createUser(newUser("user2"), Set.empty)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          res2 shouldBe Right(createdUser("user2"))
        }
      }
    }

    "disallow re-creating an existing user" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          res2 <- tested.createUser(user, Set.empty)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          res2 shouldBe Left(UserExists(user.id))
        }
      }
    }

    "find a freshly created user" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          user1 <- tested.getUser(user.id)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          user1 shouldBe res1
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
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          user1 <- tested.getUser("user1")
          res2 <- tested.deleteUser("user1")
          user2 <- tested.getUser("user1")
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          user1 shouldBe res1
          res2 shouldBe Right(())
          user2 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
    "allow recreating a deleted user" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          res2 <- tested.deleteUser(user.id)
          res3 <- tested.createUser(user, Set.empty)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          res2 shouldBe Right(())
          res3 shouldBe Right(createdUser("user1"))
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
          _ <- tested.createUser(newUser("user1"), Set.empty)
          _ <- tested.createUser(newUser("user2"), Set.empty)
          _ <- tested.createUser(newUser("user3"), Set.empty)
          _ <- tested.createUser(newUser("user4"), Set.empty)
          list1 <- tested.listUsers(fromExcl = None, maxResults = 3)
          _ = list1 shouldBe Right(
            UsersPage(
              Seq(
                createdUser("user1"),
                createdUser("user2"),
                createdUser("user3"),
              )
            )
          )
          list2 <- tested.listUsers(
            fromExcl = list1.getOrElse(fail("Expecting a Right()")).lastUserIdOption,
            maxResults = 4,
          )
          _ = list2 shouldBe Right(UsersPage(Seq(createdUser("user4"))))
        } yield {
          succeed
        }
      }
    }
    "not list deleted users" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(newUser("user1"), Set.empty)
          res2 <- tested.createUser(newUser("user2"), Set.empty)
          users1 <- tested.listUsers(fromExcl = None, maxResults = 10000)
          res3 <- tested.deleteUser("user1")
          users2 <- tested.listUsers(fromExcl = None, maxResults = 10000)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          res2 shouldBe Right(createdUser("user2"))
          users1 shouldBe Right(
            UsersPage(
              Seq(
                createdUser("user1"),
                createdUser("user2"),
              )
            )
          )
          res3 shouldBe Right(())
          users2 shouldBe Right(UsersPage(Seq(createdUser("user2"))))

        }
      }
    }
  }

  "user rights management" - {
    import UserRight._
    "listUserRights should find the rights of a freshly created user" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(newUser("user1"), Set.empty)
          rights1 <- tested.listUserRights("user1")
          user2 <- tested.createUser(
            newUser("user2"),
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
          )
          rights2 <- tested.listUserRights("user2")
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          rights1 shouldBe Right(Set.empty)
          user2 shouldBe Right(createdUser("user2"))
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
          res1 <- tested.createUser(newUser("user1"), Set.empty)
          rights1 <- tested.grantRights("user1", Set(ParticipantAdmin))
          rights2 <- tested.grantRights("user1", Set(ParticipantAdmin))
          rights3 <- tested.grantRights("user1", Set(CanActAs("party1"), CanReadAs("party2")))
          rights4 <- tested.listUserRights("user1")
        } yield {
          res1 shouldBe Right(createdUser("user1"))
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
            newUser("user1"),
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
          )
          rights1 <- tested.listUserRights("user1")
          rights2 <- tested.revokeRights("user1", Set(ParticipantAdmin))
          rights3 <- tested.revokeRights("user1", Set(ParticipantAdmin))
          rights4 <- tested.listUserRights("user1")
          rights5 <- tested.revokeRights("user1", Set(CanActAs("party1"), CanReadAs("party2")))
          rights6 <- tested.listUserRights("user1")
        } yield {
          res1 shouldBe Right(createdUser("user1"))
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

  "updating" - {
    "update an existing user" in {
      testIt { tested =>
        val pr1 = newUser("user1")
        for {
          create1 <- tested.createUser(pr1, Set.empty)
          _ = create1.value shouldBe createdUser("user1")
          update1 <- tested.updateUser(
            userUpdate = UserUpdate(
              id = pr1.id,
              // TODO um-for-hub: Test primaryPartyUpdate and isDeactivatedUpdate
              primaryPartyUpdateO = None,
              isDeactivatedUpdateO = None,
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = create1.value.metadata.resourceVersionO,
                annotationsUpdateO = Some(Merge.fromNonEmpty(Map("k1" -> "v1"))),
              ),
            )
          )
          _ = resetResourceVersion(update1.value) shouldBe newUser(
            "user1",
            annotations = Map("k1" -> "v1"),
          )
        } yield succeed
      }
    }

    "should update metadata annotations with merge and replace semantics" in {
      testIt { tested =>
        val user = createdUser("user1", annotations = Map("k1" -> "v1", "k2" -> "v2"))
        for {
          create1 <- tested.createUser(user, Set.empty)
          _ = create1.value shouldBe createdUser(
            "user1",
            annotations = Map("k1" -> "v1", "k2" -> "v2"),
          )
          // first update: with merge annotations semantics
          update1 <- tested.updateUser(
            UserUpdate(
              id = user.id,
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = None,
                annotationsUpdateO = Some(
                  Merge.fromNonEmpty(
                    Map(
                      "k1" -> "v1b",
                      "k3" -> "v3",
                    )
                  )
                ),
              ),
            )
          )
          _ = update1.value shouldBe createdUser(
            "user1",
            resourceVersion = 1,
            annotations = Map("k1" -> "v1b", "k2" -> "v2", "k3" -> "v3"),
          )
          // second update: with replace annotations semantics
          update2 <- tested.updateUser(
            UserUpdate(
              id = user.id,
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = None,
                annotationsUpdateO = Some(
                  Replace(
                    Map(
                      "k1" -> "v1c",
                      "k4" -> "v4",
                    )
                  )
                ),
              ),
            )
          )
          _ = update2.value shouldBe createdUser(
            "user1",
            resourceVersion = 2,
            annotations = Map("k1" -> "v1c", "k4" -> "v4"),
          )
          // third update: with replace annotations semantics - effectively deleting all annotations
          update3 <- tested.updateUser(
            UserUpdate(
              id = user.id,
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = None,
                annotationsUpdateO = Some(Replace(Map.empty)),
              ),
            )
          )
          _ = update3.value shouldBe createdUser(
            "user1",
            resourceVersion = 3,
            annotations = Map.empty,
          )
        } yield {
          succeed
        }
      }
    }

    "should raise error when updating a non-existing user" in {
      testIt { tested =>
        val userId = Ref.UserId.assertFromString("user")
        for {
          res1 <- tested.updateUser(
            UserUpdate(
              id = userId,
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = None,
                annotationsUpdateO = Some(Merge.fromNonEmpty(Map("k1" -> "v1"))),
              ),
            )
          )
          _ = res1.left.value shouldBe UserManagementStore.UserNotFound(userId)
        } yield succeed
      }
    }

    "should raise an error on resource version mismatch" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          _ <- tested.createUser(user, Set.empty)
          res1 <- tested.updateUser(
            UserUpdate(
              id = user.id,
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = Some(100),
                annotationsUpdateO = Some(Merge.fromNonEmpty(Map("k1" -> "v1"))),
              ),
            )
          )
          _ = res1.left.value shouldBe UserManagementStore.ConcurrentUserUpdate(user.id)
        } yield succeed
      }
    }

    "raise an error when annotations byte size max size exceeded" - {
      // This value consumes just a bit over half the allowed max size limit
      val bigValue = "big value:" + ("a" * 128 * 1024)

      "when creating a user" in {
        testIt { tested =>
          val user = newUser(annotations = Map("k1" -> bigValue, "k2" -> bigValue))
          for {
            res1 <- tested.createUser(user, Set.empty)
            _ = res1.left.value shouldBe MaxAnnotationsSizeExceeded(user.id)
          } yield succeed
        }
      }

      "when updating an existing user" in {
        testIt { tested =>
          val user = newUser(annotations = Map("k1" -> bigValue))
          for {
            _ <- tested.createUser(user, Set.empty)
            res1 <- tested.updateUser(
              makeUserUpdate(annotationsUpdateO = Some(Merge.fromNonEmpty(Map("k2" -> bigValue))))
            )
            _ = res1.left.value shouldBe MaxAnnotationsSizeExceeded(user.id)
          } yield succeed
        }
      }
    }

  }

}
