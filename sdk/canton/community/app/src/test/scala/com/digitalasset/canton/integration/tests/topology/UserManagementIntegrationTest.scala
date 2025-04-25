// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.{
  ModifyingNonModifiableUserPropertiesError,
  User,
  UserRights,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.UserManagementServiceErrors.{
  UserAlreadyExists,
  UserNotFound,
}
import com.digitalasset.canton.topology.PartyId
import monocle.macros.syntax.lens.*

import java.util.UUID

private object UserManagementIntegrationTest {
  val extraAdmin: String = "this-is-extra-admin-user" + UUID.randomUUID().toString
}

// TODO(#10819)
// The API participant1.parties.enable is asynchronous, and the allocation of the party may not occur immediately
// after the API call returns. This is due to the complexity of synchronizing a number of party storages within
// the canton's landscape. After the issue is resolved, the `eventually()` function within this test and eager
// party notification can be removed.
trait UserManagementIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  import UserManagementIntegrationTest.*

  private var alice: PartyId = _
  private var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.userManagementService.additionalAdminUserId).replace(Some(extraAdmin))
        )
      )
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)

        alice = participant1.parties.enable("alice")
        bob = participant1.parties.enable("bob")
      }

  "managing users" should {

    "have extra admin user" in { implicit env =>
      import env.*
      participant1.ledger_api.users.get(extraAdmin) shouldBe User(
        id = extraAdmin,
        primaryParty = None,
        isDeactivated = false,
        annotations = Map(),
        identityProviderId = "",
      )
      participant1.ledger_api.users.rights.list(extraAdmin) shouldBe UserRights(
        actAs = Set.empty,
        readAs = Set.empty,
        readAsAnyParty = false,
        participantAdmin = true,
        identityProviderAdmin = false,
      )
    }

    "support adding and getting users" in { implicit env =>
      import env.*

      participant1.ledger_api.users.create(
        id = "admin1",
        actAs = Set(alice),
        primaryParty = Some(alice),
        readAs = Set(bob),
        participantAdmin = true,
        isDeactivated = true,
        annotations = Map("k" -> "v"),
      )
      val actualUser = participant1.ledger_api.users.get("admin1")
      actualUser shouldBe User(
        id = "admin1",
        primaryParty = Some(alice),
        isDeactivated = true,
        annotations = Map("k" -> "v"),
        identityProviderId = "",
      )
      participant1.ledger_api.users.rights.list("admin1") shouldBe UserRights(
        actAs = Set(alice),
        readAs = Set(bob),
        readAsAnyParty = false,
        participantAdmin = true,
        identityProviderAdmin = false,
      )
    }

    "support adding and listing users" in { implicit env =>
      import env.*
      participant1.ledger_api.users.create(
        id = "aliceuser",
        actAs = Set(alice),
        primaryParty = Some(alice),
        readAs = Set.empty,
        annotations = Map("k" -> "v"),
      )
      val actualUser =
        participant1.ledger_api.users.list().users.find(_.id == "aliceuser").value
      actualUser shouldBe User(
        id = "aliceuser",
        primaryParty = Some(alice),
        isDeactivated = false,
        annotations = Map("k" -> "v"),
        identityProviderId = "",
      )
    }

    "support updating users" in { implicit env =>
      import env.*
      val createdUser = participant1.ledger_api.users.create(
        id = "user1",
        primaryParty = Some(alice),
        isDeactivated = false,
        annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"),
      )
      val updatedUser = participant1.ledger_api.users.update(
        id = createdUser.id,
        modifier = (user) => {
          user.copy(
            primaryParty = Some(bob),
            isDeactivated = false,
            annotations =
              user.annotations.updated("k2", "v2a").updated("k4", "v4").removed("k3").removed("k5"),
          )
        },
      )
      updatedUser shouldBe User(
        id = "user1",
        primaryParty = Some(bob),
        isDeactivated = false,
        annotations = Map("k1" -> "v1", "k2" -> "v2a", "k4" -> "v4"),
        identityProviderId = "",
      )
    }

    "behave orderly when trying to change a non-modifiable field" in { implicit env =>
      import env.*
      val user = participant1.ledger_api.users.create(id = "user2")
      loggerFactory.assertThrowsAndLogs[ModifyingNonModifiableUserPropertiesError](
        participant1.ledger_api.users.update(id = user.id, _.copy(id = "adifferentid"))
      )
    }

    "behave orderly when updating non-existent user" in { implicit env =>
      import env.*
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.users.update(id = "unknownuser", _.copy(primaryParty = None)),
        _.shouldBeCantonErrorCode(UserNotFound),
      )
    }

    "behave orderly when getting non-existent user" in { implicit env =>
      import env.*
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.users.get("unknownuser"),
        _.shouldBeCantonErrorCode(UserNotFound),
      )
    }

    "behave orderly on duplicate adds" in { implicit env =>
      import env.*
      participant1.ledger_api.users.create(
        id = "duplo",
        Set(),
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.users.create(
          id = "duplo",
          Set(),
        ),
        _.shouldBeCantonErrorCode(UserAlreadyExists),
      )
    }

    "support removing users" in { implicit env =>
      import env.*

      participant1.ledger_api.users.delete("admin1")
      participant1.ledger_api.users.list().users.map(_.id) should not contain "admin1"

    }

    "behave orderly when removing non-existent user" in { implicit env =>
      import env.*
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.users.delete(
          id = "nothere"
        ),
        _.shouldBeCantonErrorCode(UserNotFound),
      )

    }

    "support user operations using a non-default identity providers" in { implicit env =>
      import env.*
      val lapi_idp = participant1.ledger_api.identity_provider_config
      val lapi_users = participant1.ledger_api.users
      val idpId = "idp-id-" + UUID.randomUUID().toString
      val _ = lapi_idp.create(
        identityProviderId = idpId,
        jwksUrl = "https://jwks:900",
        issuer = UUID.randomUUID().toString,
        audience = Option("someAudience"),
      )
      val userId = "user" + UUID.randomUUID().toString
      lapi_users.create(
        id = userId,
        actAs = Set(alice),
        identityProviderId = idpId,
      )
      val user = lapi_users.get(id = userId, identityProviderId = idpId)
      user.identityProviderId shouldBe idpId withClue "get"
      user.id shouldBe userId withClue "get"
      val _: User = lapi_users.list(identityProviderId = idpId).users.find(_.id == userId).value
      val _ =
        lapi_users.rights.grant(id = userId, identityProviderId = idpId, actAs = Set(alice, bob))
      val _ = lapi_users.rights.revoke(id = userId, identityProviderId = idpId, actAs = Set(alice))
      lapi_users.rights.list(id = userId, identityProviderId = idpId) shouldBe UserRights(
        actAs = Set(bob),
        readAs = Set.empty,
        readAsAnyParty = false,
        participantAdmin = false,
        identityProviderAdmin = false,
      )
      lapi_users
        .update(id = userId, identityProviderId = idpId, modifier = _.copy(isDeactivated = true))
        .isDeactivated shouldBe true
      lapi_users.delete(id = userId, identityProviderId = idpId)
      lapi_users.list(identityProviderId = idpId).users.find(_.id == userId) shouldBe None
      // Cleanup idp
      lapi_idp.delete(identityProviderId = idpId)
    }

    "support updating user's idp id" in { implicit env =>
      import env.*
      val lapi_idp = participant1.ledger_api.identity_provider_config
      val lapi_users = participant1.ledger_api.users
      val idpId1 = "idp-id1-" + UUID.randomUUID().toString
      val idpId2 = "idp-id2-" + UUID.randomUUID().toString
      val _ = lapi_idp.create(
        identityProviderId = idpId1,
        jwksUrl = "https://jwks:900",
        issuer = UUID.randomUUID().toString,
        audience = Option("someAudience"),
      )
      val _ = lapi_idp.create(
        identityProviderId = idpId2,
        jwksUrl = "https://jwks:900",
        issuer = UUID.randomUUID().toString,
        audience = Option("someAudience"),
      )
      val userId = "user" + UUID.randomUUID().toString
      lapi_users.create(
        id = userId,
        actAs = Set(alice),
        identityProviderId = "",
      )
      lapi_users.update_idp(
        id = userId,
        sourceIdentityProviderId = "",
        targetIdentityProviderId = idpId1,
      )
      val _ = {
        val user = lapi_users.get(id = userId, identityProviderId = idpId1)
        user.id shouldBe userId
        user.identityProviderId shouldBe idpId1
      }
      lapi_users.update_idp(
        id = userId,
        sourceIdentityProviderId = idpId1,
        targetIdentityProviderId = idpId2,
      )
      val _ = {
        val user = lapi_users.get(id = userId, identityProviderId = idpId2)
        user.id shouldBe userId
        user.identityProviderId shouldBe idpId2
      }
      // Cleanup user and idps
      lapi_users.delete(id = userId, identityProviderId = idpId2)
      lapi_idp.delete(identityProviderId = idpId1)
      lapi_idp.delete(identityProviderId = idpId2)
    }

    "support adding a super-reader user" in { implicit env =>
      import env.*
      participant1.ledger_api.users.create(
        id = "super-reader-user",
        readAsAnyParty = true,
      )
      participant1.ledger_api.users.rights.list("super-reader-user") shouldBe UserRights(
        actAs = Set(),
        readAs = Set(),
        readAsAnyParty = true,
        participantAdmin = false,
        identityProviderAdmin = false,
      )
    }

  }

  "managing rights" should {
    "allow granting to existing user" in { implicit env =>
      import env.*

      val david = participant1.parties.enable("david")
      val erwin = participant1.parties.enable("erwin")

      noException should be thrownBy participant1.ledger_api.users.create(
        id = "admin1",
        actAs = Set(david),
        primaryParty = Some(david),
        readAs = Set(),
      )

      participant1.ledger_api.users.rights.list("admin1") shouldBe UserRights(
        actAs = Set(david),
        readAs = Set(),
        readAsAnyParty = false,
        participantAdmin = false,
        identityProviderAdmin = false,
      )

      participant1.ledger_api.users.rights
        .grant("admin1", actAs = Set(), readAs = Set(erwin), participantAdmin = true)

      participant1.ledger_api.users.rights.list("admin1") shouldBe UserRights(
        actAs = Set(david),
        readAs = Set(erwin),
        readAsAnyParty = false,
        participantAdmin = true,
        identityProviderAdmin = false,
      )

      participant1.ledger_api.users.rights
        .grant("admin1", actAs = Set(), readAs = Set(), identityProviderAdmin = true)

      participant1.ledger_api.users.rights.list("admin1") shouldBe UserRights(
        actAs = Set(david),
        readAs = Set(erwin),
        readAsAnyParty = false,
        participantAdmin = true,
        identityProviderAdmin = true,
      )
    }

    "support removing rights" in { implicit env =>
      import env.*

      // look into authorized store, so we don't have to wait until stuff propagated
      def findParty(str: String) = participant1.topology.party_to_participant_mappings
        .list(daId, filterParty = str)
        .map(_.item.partyId)
        .headOption
        .valueOrFail(s"where is $str")

      val erwin = findParty("erwin")
      val david = findParty("david")

      participant1.ledger_api.users.rights.list("admin1") shouldBe UserRights(
        actAs = Set(david),
        readAs = Set(erwin),
        readAsAnyParty = false,
        participantAdmin = true,
        identityProviderAdmin = true,
      )

      participant1.ledger_api.users.rights
        .revoke("admin1", actAs = Set(), readAs = Set(erwin), participantAdmin = true)

      participant1.ledger_api.users.rights.list("admin1") shouldBe UserRights(
        actAs = Set(david),
        readAs = Set(),
        readAsAnyParty = false,
        participantAdmin = false,
        identityProviderAdmin = true,
      )

      participant1.ledger_api.users.rights
        .revoke("admin1", actAs = Set(), readAs = Set(), identityProviderAdmin = true)

      participant1.ledger_api.users.rights.list("admin1") shouldBe UserRights(
        actAs = Set(david),
        readAs = Set(),
        readAsAnyParty = false,
        participantAdmin = false,
        identityProviderAdmin = false,
      )
    }
  }
}

class UserManagementReferenceIntegrationTestDefault extends UserManagementIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

class UserManagementReferenceIntegrationTestPostgres extends UserManagementIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait UserManagementNoExtraAdminIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  import UserManagementIntegrationTest.*

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.userManagementService.additionalAdminUserId).replace(None)
        )
      )

  "managing users" should {
    "not have extra admin user" in { implicit env =>
      import env.*
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.users.get(extraAdmin),
        _.shouldBeCantonErrorCode(UserNotFound),
      )
    }
  }
}

class UserManagementNoExtraAdminReferenceIntegrationTestPostgres
    extends UserManagementNoExtraAdminIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
