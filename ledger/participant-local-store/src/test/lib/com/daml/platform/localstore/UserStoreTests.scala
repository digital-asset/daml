// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  JwksUrl,
  ObjectMeta,
  User,
  UserRight,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{LedgerString, Party, UserId}
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.UserManagementStore.{
  MaxAnnotationsSizeExceeded,
  UserExists,
  UserNotFound,
  UsersPage,
}
import com.daml.platform.localstore.api.{ObjectMetaUpdate, UserManagementStore, UserUpdate}
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

  val persistedIdentityProviderId =
    IdentityProviderId.Id(LedgerString.assertFromString("idp1"))
  val idp1 = IdentityProviderConfig(
    identityProviderId = persistedIdentityProviderId,
    isDeactivated = false,
    jwksUrl = JwksUrl("http://domain.com/"),
    issuer = "issuer",
  )

  def newUser(
      name: String = userId1,
      primaryParty: Option[Ref.Party] = None,
      isDeactivated: Boolean = false,
      annotations: Map[String, String] = Map.empty,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): User = User(
    id = name,
    primaryParty = primaryParty,
    isDeactivated = isDeactivated,
    metadata = ObjectMeta(
      resourceVersionO = None,
      annotations = annotations,
    ),
    identityProviderId = identityProviderId,
  )

  def createdUser(
      name: String = userId1,
      primaryParty: Option[Ref.Party] = None,
      isDeactivated: Boolean = false,
      resourceVersion: Long = 0,
      annotations: Map[String, String] = Map.empty,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): User =
    User(
      id = name,
      primaryParty = primaryParty,
      isDeactivated = isDeactivated,
      metadata = ObjectMeta(
        resourceVersionO = Some(resourceVersion),
        annotations = annotations,
      ),
      identityProviderId = identityProviderId,
    )

  def makeUserUpdate(
      id: String = userId1,
      primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
      isDeactivatedUpdateO: Option[Boolean] = None,
      annotationsUpdateO: Option[Map[String, String]] = None,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): UserUpdate = UserUpdate(
    id = id,
    primaryPartyUpdateO = primaryPartyUpdateO,
    isDeactivatedUpdateO = isDeactivatedUpdateO,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
    identityProviderId = identityProviderId,
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
          _ <- createIdentityProviderConfig(idp1)
          res3 <- tested.createUser(
            newUser("user3", identityProviderId = persistedIdentityProviderId),
            Set.empty,
          )
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          res2 shouldBe Right(createdUser("user2"))
          res3 shouldBe Right(
            createdUser("user3", identityProviderId = persistedIdentityProviderId)
          )
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
          list1 <- tested.listUsers(
            fromExcl = None,
            maxResults = 3,
            identityProviderId = IdentityProviderId.Default,
          )
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
            identityProviderId = IdentityProviderId.Default,
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
          users1 <- tested.listUsers(
            fromExcl = None,
            maxResults = 10000,
            identityProviderId = IdentityProviderId.Default,
          )
          res3 <- tested.deleteUser("user1")
          users2 <- tested.listUsers(
            fromExcl = None,
            maxResults = 10000,
            identityProviderId = IdentityProviderId.Default,
          )
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
    "list users within idp" in {
      testIt { tested =>
        for {
          _ <- createIdentityProviderConfig(idp1)
          _ <- tested.createUser(
            newUser("user1", identityProviderId = persistedIdentityProviderId),
            Set.empty,
          )
          _ <- tested.createUser(newUser("user2"), Set.empty)
          _ <- tested.createUser(newUser("user3"), Set.empty)
          _ <- tested.createUser(
            newUser("user4", identityProviderId = persistedIdentityProviderId),
            Set.empty,
          )
          list1 <- tested.listUsers(
            fromExcl = None,
            maxResults = 3,
            identityProviderId = persistedIdentityProviderId,
          )
          _ = list1 shouldBe Right(
            UsersPage(
              Seq(
                createdUser("user1", identityProviderId = persistedIdentityProviderId),
                createdUser("user4", identityProviderId = persistedIdentityProviderId),
              )
            )
          )
          list2 <- tested.listUsers(
            fromExcl = None,
            maxResults = 4,
            identityProviderId = IdentityProviderId.Default,
          )
          _ = list2 shouldBe Right(
            UsersPage(
              Seq(
                createdUser("user2"),
                createdUser("user3"),
              )
            )
          )
        } yield {
          succeed
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
          rights3 <- tested.grantRights(
            "user1",
            Set(CanActAs("party1"), CanReadAs("party2")),
          )
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
          rights5 <- tested.revokeRights(
            "user1",
            Set(CanActAs("party1"), CanReadAs("party2")),
          )
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
    "update an existing user's annotations" in {
      testIt { tested =>
        val pr1 = newUser("user1", isDeactivated = true, primaryParty = None)
        for {
          create1 <- tested.createUser(pr1, Set.empty)
          _ = create1.value shouldBe createdUser("user1", isDeactivated = true)
          update1 <- tested.updateUser(
            userUpdate = UserUpdate(
              id = pr1.id,
              primaryPartyUpdateO = Some(Some(Ref.Party.assertFromString("party123"))),
              isDeactivatedUpdateO = Some(false),
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = create1.value.metadata.resourceVersionO,
                annotationsUpdateO = Some(Map("k1" -> "v1")),
              ),
              identityProviderId = pr1.identityProviderId,
            )
          )
          _ = resetResourceVersion(update1.value) shouldBe newUser(
            "user1",
            primaryParty = Some(Ref.Party.assertFromString("party123")),
            isDeactivated = false,
            annotations = Map("k1" -> "v1"),
          )
        } yield succeed
      }
    }

    "should update metadata annotations" in {
      testIt { tested =>
        val user = newUser("user1", annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))
        for {
          _ <- tested.createUser(user, Set.empty)
          update1 <- tested.updateUser(
            UserUpdate(
              id = user.id,
              metadataUpdate = ObjectMetaUpdate(
                resourceVersionO = None,
                annotationsUpdateO = Some(
                  Map(
                    "k1" -> "v1a",
                    "k3" -> "",
                    "k4" -> "v4",
                  )
                ),
              ),
              identityProviderId = user.identityProviderId,
            )
          )
          _ = update1.value shouldBe createdUser(
            "user1",
            resourceVersion = 1,
            annotations = Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
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
                annotationsUpdateO = Some(Map("k1" -> "v1")),
              ),
              identityProviderId = IdentityProviderId.Default,
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
                annotationsUpdateO = Some(Map("k1" -> "v1")),
              ),
              identityProviderId = user.identityProviderId,
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
              makeUserUpdate(annotationsUpdateO = Some(Map("k2" -> bigValue)))
            )
            _ = res1.left.value shouldBe MaxAnnotationsSizeExceeded(user.id)
          } yield succeed
        }
      }
    }

  }

}
