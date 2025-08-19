// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ImpliedIdpUserManagementServiceTest extends ServiceCallAuthTests with ImpliedIdpFixture {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "<N/A>"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  "user management service" should {

    "imply idp id when no idp id in the request from a regular user" in { implicit env =>
      val userId1 = "user-regular-" + UUID.randomUUID().toString
      val idpId1 = "idp1-" + UUID.randomUUID().toString
      impliedIdpWithRegularUserFixture(idpId1, userId = userId1) { (context: ServiceCallContext) =>
        import env.*
        val userManagement = stub(token = context.token)
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          for {
            getUserResp <- userManagement.getUser(
              GetUserRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
            _ = {
              val user = getUserResp.user.value
              user.id shouldBe userId1 withClue "get user"
              user.identityProviderId shouldBe idpId1 withClue "get user"
            }
            // This establishes that only one user with userId1 exists, and we know it belongs to idpId1.
            // Thanks to that, when testing the remaining user management service calls, we can say
            // that the idp id was correctly implied just by the mere fact the those calls succeeded.
            _ <- ensureOneUserWithGivenIdExists(nonDefaultIdp = idpId1, userId = userId1)
            _ <- userManagement.listUserRights(
              ListUserRightsRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
          } yield succeed
        }
      }
    }

    "imply idp id when no idp id in the request from an idp admin user" in { implicit env =>
      val idpId1 = "idp1-" + UUID.randomUUID().toString
      impliedIdpWithIdpAdminFixture(idpId1) { (context: ServiceCallContext) =>
        import env.*
        val userId1 = "userId1-" + UUID.randomUUID().toString
        val userManagement = stub(token = context.token)
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          for {
            createUserResp <- userManagement.createUser(
              CreateUserRequest(
                user = Some(
                  User(
                    id = userId1,
                    primaryParty = "",
                    isDeactivated = false,
                    metadata = None,
                    identityProviderId = "",
                  )
                ),
                rights = Nil,
              )
            )
            _ = {
              val user = createUserResp.user.value
              user.id shouldBe userId1 withClue "create user"
              user.identityProviderId shouldBe idpId1 withClue "create user"
            }
            listUsersResp <- userManagement.listUsers(
              ListUsersRequest(
                pageToken = "",
                pageSize = 0,
                identityProviderId = "",
              )
            )
            _ = {
              val user = listUsersResp.users.find(_.id == userId1).value
              user.id shouldBe userId1 withClue "list users"
              user.identityProviderId shouldBe idpId1 withClue "list users"
            }
            // This establishes that only one user with userId1 exists, and we know it belongs to idpId1.
            // Thanks to that, when testing the remaining user management service calls, we can say
            // that that the idp id was correctly implied just be the mere fact the those calls succeeded.
            _ <- ensureOneUserWithGivenIdExists(nonDefaultIdp = idpId1, userId = userId1)
            _ <- userManagement.getUser(
              GetUserRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
            _ <- userManagement.updateUser(
              UpdateUserRequest(
                Some(
                  User(
                    id = userId1,
                    primaryParty = "alice123",
                    isDeactivated = false,
                    metadata = None,
                    identityProviderId = "",
                  )
                ),
                updateMask = Some(FieldMask(paths = Seq("primary_party"))),
              )
            )
            _ <- userManagement.grantUserRights(
              GrantUserRightsRequest(
                userId = userId1,
                identityProviderId = "",
                rights = Seq.empty,
              )
            )
            _ <- userManagement.revokeUserRights(
              RevokeUserRightsRequest(
                userId = userId1,
                identityProviderId = "",
                rights = Seq.empty,
              )
            )
            _ <- userManagement.listUserRights(
              ListUserRightsRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
            _ <- userManagement.deleteUser(
              DeleteUserRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
          } yield List.empty -> List.empty
        }
      }
    }

    "not imply idp id when no idp id in the request from an participant admin user" in {
      implicit env =>
        val idpId1 = "idp1-" + UUID.randomUUID().toString
        impliedIdpWithParticipantAdminFixture(idpId1) { (context: ServiceCallContext) =>
          import env.*
          val userId1 = "userId1-" + UUID.randomUUID().toString
          val userManagement = stub(token = context.token)
          for {
            createUserResp <- userManagement.createUser(
              CreateUserRequest(
                user = Some(
                  User(
                    id = userId1,
                    primaryParty = "",
                    isDeactivated = false,
                    metadata = None,
                    identityProviderId = "",
                  )
                ),
                rights = Nil,
              )
            )
            _ = {
              val user = createUserResp.user.value
              user.id shouldBe userId1 withClue "create user"
              user.identityProviderId shouldBe "" withClue "create user"
            }
            listUsersResp <- userManagement.listUsers(
              ListUsersRequest(
                pageToken = "",
                pageSize = 0,
                identityProviderId = "",
              )
            )
            _ = {
              val user = listUsersResp.users.find(_.id == userId1).value
              user.id shouldBe userId1 withClue "list users"
              user.identityProviderId shouldBe "" withClue "list users"
            }
            // This establishes that only one user with userId1 exists, and we know it belongs to the default idp.
            // Thanks to that, when testing the remaining user management service calls, we can say
            // that that the idp id was not implied just be the mere fact the those calls succeeded.
            _ <- ensureOneUserWithGivenIdExists(nonDefaultIdp = idpId1, userId = userId1)
            _ <- userManagement.getUser(
              GetUserRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
            _ <- userManagement.updateUser(
              UpdateUserRequest(
                Some(
                  User(
                    id = userId1,
                    primaryParty = "alice123",
                    isDeactivated = false,
                    metadata = None,
                    identityProviderId = "",
                  )
                ),
                updateMask = Some(FieldMask(paths = Seq("primary_party"))),
              )
            )
            _ <- userManagement.grantUserRights(
              GrantUserRightsRequest(
                userId = userId1,
                identityProviderId = "",
                rights = Seq.empty,
              )
            )
            _ <- userManagement.revokeUserRights(
              RevokeUserRightsRequest(
                userId = userId1,
                identityProviderId = "",
                rights = Seq.empty,
              )
            )
            _ <- userManagement.listUserRights(
              ListUserRightsRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
            _ <- userManagement.deleteUser(
              DeleteUserRequest(
                userId = userId1,
                identityProviderId = "",
              )
            )
          } yield succeed
        }
    }
  }

  private def ensureOneUserWithGivenIdExists(nonDefaultIdp: String, userId: String)(implicit
      ec: ExecutionContext
  ): Future[Assertion] = {
    val userManagementAdmin = stub(token = canBeAnAdmin.token)
    for {
      users1 <- userManagementAdmin
        .listUsers(
          ListUsersRequest(
            pageToken = "",
            pageSize = 0,
            identityProviderId = "",
          )
        )
        .map(_.users.filter(_.id == userId))
      users2 <- userManagementAdmin
        .listUsers(
          ListUsersRequest(
            pageToken = "",
            pageSize = 0,
            identityProviderId = nonDefaultIdp,
          )
        )
        .map(_.users.filter(_.id == userId))
    } yield {
      val users = (users1 ++ users2).distinct
      users should have size 1 withClue s"exactly one user with id $userId exists $users"
    }
  }
}
