// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{LedgerString, Party, UserId}
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  JwksUrl,
  ObjectMeta,
  User,
  UserRight,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore.*
import com.digitalasset.canton.ledger.localstore.api.{
  ObjectMetaUpdate,
  UserManagementStore,
  UserUpdate,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future
import scala.language.implicitConversions

/** Common tests for implementations of [[UserManagementStore]]
  */
trait UserStoreTests extends UserStoreSpecBase { self: AsyncFreeSpec =>

  implicit val lc: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)

  private implicit def toUserId(s: String): UserId =
    UserId.assertFromString(s)

  private val userId1 = "user1"

  private val idpId1 = IdentityProviderId.Id(LedgerString.assertFromString("idp1"))
  private val idpId2 = IdentityProviderId.Id(LedgerString.assertFromString("idp2"))
  private val defaultIdpId = IdentityProviderId.Default
  private val idp1 = IdentityProviderConfig(
    identityProviderId = idpId1,
    isDeactivated = false,
    jwksUrl = JwksUrl("http://domain.com/"),
    issuer = "issuer",
    audience = Some("audience"),
  )
  private val idp2 = IdentityProviderConfig(
    identityProviderId = idpId2,
    isDeactivated = false,
    jwksUrl = JwksUrl("http://domain2.com/"),
    issuer = "issuer2",
    audience = Some("audience"),
  )

  def newUser(
      name: String = userId1,
      primaryParty: Option[Ref.Party] = None,
      isDeactivated: Boolean = false,
      annotations: Map[String, String] = Map.empty,
      identityProviderId: IdentityProviderId = defaultIdpId,
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
            newUser("user3", identityProviderId = idpId1),
            Set.empty,
          )
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          res2 shouldBe Right(createdUser("user2"))
          res3 shouldBe Right(
            createdUser("user3", identityProviderId = idpId1)
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

    "disallow re-creating an existing user concurrently" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res <- Future.sequence(
            Seq(tested.createUser(user, Set.empty), tested.createUser(user, Set.empty))
          )
        } yield {
          res should contain(Left(UserExists(user.id)))
        }
      }
    }

    "deny permission re-creating an existing user within another IDP" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          res2 <- tested.createUser(
            user.copy(identityProviderId = idpId1),
            Set.empty,
          )
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          res2 shouldBe Left(PermissionDenied(user.id))
        }
      }
    }

    "find a freshly created user" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          user1 <- tested.getUser(user.id, defaultIdpId)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          user1 shouldBe res1
        }
      }
    }

    "deny to find a freshly created user within another IDP" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          user1 <- tested.getUser(user.id, idpId1)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          user1 shouldBe Left(PermissionDenied(user.id))
        }
      }
    }

    "not find a non-existent user" in {
      testIt { tested =>
        val userId: Ref.UserId = "user1"
        for {
          user1 <- tested.getUser(userId, defaultIdpId)
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
          user1 <- tested.getUser("user1", defaultIdpId)
          res2 <- tested.deleteUser("user1", defaultIdpId)
          user2 <- tested.getUser("user1", defaultIdpId)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          user1 shouldBe res1
          res2 shouldBe Right(())
          user2 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
    "deny to delete user within another IDP" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          user1 <- tested.getUser("user1", idpId1)
        } yield {
          res1 shouldBe Right(createdUser("user1"))
          user1 shouldBe Left(PermissionDenied(user.id))
        }
      }
    }
    "allow recreating a deleted user" in {
      testIt { tested =>
        val user = newUser("user1")
        for {
          res1 <- tested.createUser(user, Set.empty)
          res2 <- tested.deleteUser(user.id, defaultIdpId)
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
          res1 <- tested.deleteUser("user1", defaultIdpId)
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
          res3 <- tested.deleteUser("user1", defaultIdpId)
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
            newUser("user1", identityProviderId = idpId1),
            Set.empty,
          )
          _ <- tested.createUser(newUser("user2"), Set.empty)
          _ <- tested.createUser(newUser("user3"), Set.empty)
          _ <- tested.createUser(
            newUser("user4", identityProviderId = idpId1),
            Set.empty,
          )
          list1 <- tested.listUsers(
            fromExcl = None,
            maxResults = 3,
            identityProviderId = idpId1,
          )
          _ = list1 shouldBe Right(
            UsersPage(
              Seq(
                createdUser("user1", identityProviderId = idpId1),
                createdUser("user4", identityProviderId = idpId1),
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
    import UserRight.*
    "listUserRights should find the rights of a freshly created user" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(newUser("user1"), Set.empty)
          rights1 <- tested.listUserRights("user1", defaultIdpId)
          user2 <- tested.createUser(
            newUser("user2"),
            Set(ParticipantAdmin, CanActAs("party1"), CanReadAs("party2")),
          )
          rights2 <- tested.listUserRights("user2", defaultIdpId)
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
    "listUserRights should deny for user in another IDP" in {
      testIt { tested =>
        for {
          _ <- tested.createUser(newUser("user1"), Set.empty)
          rights1 <- tested.listUserRights("user1", idpId1)
        } yield {
          rights1 shouldBe Left(PermissionDenied("user1"))
        }
      }
    }
    "listUserRights should fail on non-existent user" in {
      testIt { tested =>
        for {
          rights1 <- tested.listUserRights("user1", defaultIdpId)
        } yield {
          rights1 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
    "grantUserRights should add new rights" in {
      testIt { tested =>
        for {
          res1 <- tested.createUser(newUser("user1"), Set.empty)
          rights1 <- tested.grantRights("user1", Set(ParticipantAdmin), defaultIdpId)
          rights2 <- tested.grantRights("user1", Set(ParticipantAdmin), defaultIdpId)
          rights3 <- tested.grantRights(
            "user1",
            Set(CanActAs("party1"), CanReadAs("party2")),
            defaultIdpId,
          )
          rights4 <- tested.listUserRights("user1", defaultIdpId)
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
          rights1 <- tested.grantRights("user1", Set.empty, defaultIdpId)
        } yield {
          rights1 shouldBe Left(UserNotFound("user1"))
        }

      }
    }
    "grantRights should deny for user in another IDP" in {
      testIt { tested =>
        for {
          _ <- tested.createUser(newUser("user1"), Set.empty)
          rights1 <- tested.grantRights("user1", Set.empty, idpId1)
        } yield {
          rights1 shouldBe Left(PermissionDenied("user1"))
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
          rights1 <- tested.listUserRights("user1", defaultIdpId)
          rights2 <- tested.revokeRights("user1", Set(ParticipantAdmin), defaultIdpId)
          rights3 <- tested.revokeRights("user1", Set(ParticipantAdmin), defaultIdpId)
          rights4 <- tested.listUserRights("user1", defaultIdpId)
          rights5 <- tested.revokeRights(
            "user1",
            Set(CanActAs("party1"), CanReadAs("party2")),
            defaultIdpId,
          )
          rights6 <- tested.listUserRights("user1", defaultIdpId)
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
          rights1 <- tested.revokeRights("user1", Set.empty, defaultIdpId)
        } yield {
          rights1 shouldBe Left(UserNotFound("user1"))
        }
      }
    }
    "revokeRights should deny for user in another IDP" in {
      testIt { tested =>
        for {
          _ <- tested.createUser(newUser("user1"), Set.empty)
          rights1 <- tested.revokeRights("user1", Set.empty, idpId1)
        } yield {
          rights1 shouldBe Left(PermissionDenied("user1"))
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

  "reassigning idp" - {
    "change user's idp" in {
      testIt { tested =>
        for {
          create <- tested.createUser(
            newUser("user1", identityProviderId = defaultIdpId),
            Set.empty,
          )
          _ = create.value shouldBe createdUser("user1", identityProviderId = defaultIdpId)
          _ <- createIdentityProviderConfig(idp1)
          updated <- tested.updateUserIdp(
            sourceIdp = defaultIdpId,
            targetIdp = idpId1,
            id = create.value.id,
          )
          _ <- updated.value.identityProviderId shouldBe idpId1
        } yield succeed
      }
    }

    "when using wrong source idp id" in {
      testIt { tested =>
        for {
          _ <- createIdentityProviderConfig(idp1)
          _ <- createIdentityProviderConfig(idp2)
          user = newUser("user1", identityProviderId = idpId1)
          create <- tested.createUser(user, Set.empty)
          _ = create.value shouldBe createdUser("user1", identityProviderId = idpId1)
          updateResult <- tested.updateUserIdp(
            sourceIdp = idpId2,
            targetIdp = defaultIdpId,
            id = create.value.id,
          )
          _ <- updateResult.left.value shouldBe PermissionDenied(user.id)
        } yield succeed
      }
    }

    "cannot change idp for non-existent user" in {
      testIt { tested =>
        val userId: Ref.UserId = "user1"
        for {
          updated <- tested.updateUserIdp(
            sourceIdp = defaultIdpId,
            targetIdp = idpId1,
            id = userId,
          )
          _ <- updated.left.value shouldBe UserNotFound(userId)
        } yield succeed
      }
    }

  }
}
