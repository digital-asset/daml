// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors

import java.util.UUID
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.v1.admin.identity_provider_config_service.{
  CreateIdentityProviderConfigRequest,
  DeleteIdentityProviderConfigRequest,
  DeleteIdentityProviderConfigResponse,
  GetIdentityProviderConfigRequest,
  IdentityProviderConfig,
  UpdateIdentityProviderConfigRequest,
}
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.Future

class IdentityProviderConfigServiceIT extends UserManagementServiceITBase {

  test(
    "CreateConfigInvalidArguments",
    "Test argument validation for IdentityProviderConfigService#CreateIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def createAndCheck(
        problem: String,
        expectedErrorCode: ErrorCode,
        identityProviderId: String = UUID.randomUUID().toString,
        isDeactivated: Boolean = false,
        issuer: String = UUID.randomUUID().toString,
        jwksUrl: String = "http://daml.com/jwks.json",
    ): Future[Unit] = ledger
      .createIdentityProviderConfig(
        identityProviderId,
        isDeactivated,
        issuer,
        jwksUrl,
      )
      .mustFailWith(context = problem, expectedErrorCode)

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.MissingField,
        identityProviderId = "",
      )
      _ <- createAndCheck(
        "invalid identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
        identityProviderId = "!@",
      )
      _ <- createAndCheck(
        "empty issuer",
        LedgerApiErrors.RequestValidation.MissingField,
        issuer = "",
      )
      _ <- createAndCheck(
        "empty jwks_url",
        LedgerApiErrors.RequestValidation.MissingField,
        jwksUrl = "",
      )
      _ <- createAndCheck(
        "non valid jwks_url",
        LedgerApiErrors.RequestValidation.InvalidField,
        jwksUrl = "url.com",
      )
      _ <- ledger
        .createIdentityProviderConfig(CreateIdentityProviderConfigRequest(None))
        .mustFailWith(
          context = "empty identity_provider_config",
          LedgerApiErrors.RequestValidation.MissingField,
        )
    } yield ()
  })

  test(
    "GetConfigInvalidArguments",
    "Test argument validation for IdentityProviderConfigService#GetIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def createAndCheck(
        problem: String,
        expectedErrorCode: ErrorCode,
        identityProviderId: String,
    ): Future[Unit] = ledger
      .getIdentityProviderConfig(
        GetIdentityProviderConfigRequest(identityProviderId)
      )
      .mustFailWith(context = problem, expectedErrorCode)

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.MissingField,
        identityProviderId = "",
      )
      _ <- createAndCheck(
        "invalid identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
        identityProviderId = "!@",
      )
    } yield ()
  })

  test(
    "UpdateConfigInvalidArguments",
    "Test argument validation for IdentityProviderConfigService#UpdateIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def createAndCheck(
        problem: String,
        expectedErrorCode: ErrorCode,
        identityProviderId: String = UUID.randomUUID().toString,
        isDeactivated: Boolean = false,
        issuer: String = UUID.randomUUID().toString,
        jwksUrl: String = "http://daml.com/jwks.json",
        updateMask: Option[FieldMask] = Some(FieldMask(Seq("is_deactivated"))),
    ): Future[Unit] = ledger
      .updateIdentityProviderConfig(
        identityProviderId,
        isDeactivated,
        issuer,
        jwksUrl,
        updateMask,
      )
      .mustFailWith(context = problem, expectedErrorCode)

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.MissingField,
        identityProviderId = "",
      )
      _ <- createAndCheck(
        "invalid identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
        identityProviderId = "!@",
      )
      _ <- createAndCheck(
        "non valid url",
        LedgerApiErrors.RequestValidation.InvalidField,
        jwksUrl = "url.com",
      )
      _ <- createAndCheck(
        "empty update_mask",
        LedgerApiErrors.RequestValidation.MissingField,
        updateMask = None,
      )
      _ <- ledger
        .updateIdentityProviderConfig(UpdateIdentityProviderConfigRequest(None))
        .mustFailWith(
          context = "empty identity_provider_config",
          LedgerApiErrors.RequestValidation.MissingField,
        )

      createdIdp <- ledger.createIdentityProviderConfig()

      _ <- ledger
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            Some(createdIdp.identityProviderConfig.get),
            Some(FieldMask(Seq.empty)),
          )
        )
        .mustFailWith(
          context = "empty update_mask",
          LedgerApiErrors.Admin.IdentityProviderConfig.InvalidUpdateIdentityProviderConfigRequest,
        )
    } yield ()
  })

  test(
    "DeleteConfigInvalidArguments",
    "Test argument validation for IdentityProviderConfigService#DeleteIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def createAndCheck(
        problem: String,
        expectedErrorCode: ErrorCode,
        identityProviderId: String,
    ): Future[Unit] = ledger
      .deleteIdentityProviderConfig(
        DeleteIdentityProviderConfigRequest(identityProviderId)
      )
      .mustFailWith(context = problem, expectedErrorCode)

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.MissingField,
        identityProviderId = "",
      )
      _ <- createAndCheck(
        "invalid identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
        identityProviderId = "!@",
      )
    } yield ()
  })

  test(
    "CreateConfig",
    "Exercise CreateIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val identityProviderId = UUID.randomUUID().toString
    val isDeactivated = false
    val issuer = UUID.randomUUID().toString
    val jwksUrl = "http://daml.com/jwks.json"
    val config = IdentityProviderConfig(
      identityProviderId,
      isDeactivated,
      issuer,
      jwksUrl,
    )
    for {
      response1 <- ledger.createIdentityProviderConfig(
        CreateIdentityProviderConfigRequest(Some(config))
      )
      response2 <- ledger.createIdentityProviderConfig(
        isDeactivated = true
      )
      _ <- ledger
        .createIdentityProviderConfig(
          identityProviderId,
          isDeactivated,
          UUID.randomUUID().toString,
          jwksUrl,
        )
        .mustFailWith(
          "Creating duplicate IDP with the same ID",
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigAlreadyExists,
        )

      _ <- ledger
        .createIdentityProviderConfig(
          issuer = issuer
        )
        .mustFailWith(
          "Creating duplicate IDP with the same issuer",
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigIssuerAlreadyExists,
        )

    } yield {
      assertEquals(response1.identityProviderConfig, Some(config))
      assertIdentityProviderConfig(response2.identityProviderConfig) { config =>
        assertEquals(config.isDeactivated, true)
      }
    }
  })

  test(
    "UpdateConfig",
    "Exercise UpdateIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      response <- ledger.createIdentityProviderConfig()
      response2 <- ledger.createIdentityProviderConfig()
      response3 <- ledger.createIdentityProviderConfig()
      isDeactivatedUpdate <- ledger
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              response.identityProviderConfig.map(_.copy(isDeactivated = true)),
            updateMask = Some(FieldMask(Seq("is_deactivated"))),
          )
        )
      jwksUrlUpdate <- ledger
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              response.identityProviderConfig.map(_.copy(jwksUrl = "http://daml.com/jwks2.json")),
            updateMask = Some(FieldMask(Seq("jwks_url"))),
          )
        )
      newIssuer = UUID.randomUUID().toString
      issuerUpdate <- ledger
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              response.identityProviderConfig.map(_.copy(issuer = newIssuer)),
            updateMask = Some(FieldMask(Seq("issuer"))),
          )
        )

      duplicateIssuer = response2.identityProviderConfig.get.issuer
      _ <- ledger
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              response3.identityProviderConfig.map(_.copy(issuer = duplicateIssuer)),
            updateMask = Some(FieldMask(Seq("issuer"))),
          )
        )
        .mustFailWith(
          "Updating to the issuer which already exists",
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigIssuerAlreadyExists,
        )
    } yield {
      assertIdentityProviderConfig(isDeactivatedUpdate.identityProviderConfig) { config =>
        assertEquals(config.isDeactivated, true)
      }

      assertIdentityProviderConfig(jwksUrlUpdate.identityProviderConfig) { config =>
        assertEquals(config.jwksUrl, "http://daml.com/jwks2.json")
      }

      assertIdentityProviderConfig(issuerUpdate.identityProviderConfig) { config =>
        assertEquals(config.issuer, newIssuer)
      }
    }
  })

  test(
    "GetConfig",
    "Exercise GetIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val identityProviderId = UUID.randomUUID().toString
    val isDeactivated = false
    val issuer = UUID.randomUUID().toString
    val jwksUrl = "http://daml.com/jwks.json"
    val config = IdentityProviderConfig(
      identityProviderId,
      isDeactivated,
      issuer,
      jwksUrl,
    )
    for {
      response1 <- ledger.createIdentityProviderConfig(
        CreateIdentityProviderConfigRequest(Some(config))
      )
      response2 <- ledger.getIdentityProviderConfig(
        GetIdentityProviderConfigRequest(identityProviderId)
      )
      _ <- ledger
        .getIdentityProviderConfig(
          GetIdentityProviderConfigRequest(
            UUID.randomUUID().toString
          )
        )
        .mustFailWith(
          "non existing idp",
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigNotFound,
        )
    } yield {
      assertEquals(response1.identityProviderConfig, Some(config))
      assertEquals(response2.identityProviderConfig, Some(config))
    }
  })

  test(
    "ListConfig",
    "Exercise ListIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val id1 = UUID.randomUUID().toString
    val id2 = UUID.randomUUID().toString
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = id1)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = id2)
      listResponse <- ledger.listIdentityProviderConfig()
    } yield {
      val ids = listResponse.identityProviderConfigs.map(_.identityProviderId)
      assertEquals(ids.contains(id1), true)
      assertEquals(ids.contains(id2), true)
    }
  })

  test(
    "DeleteConfig",
    "Exercise DeleteIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val id = UUID.randomUUID().toString
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = id)
      deleted <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(id))
      _ <- ledger
        .deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(id))
        .mustFailWith(
          "config does not exist anymore",
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigNotFound,
        )
    } yield {
      assertEquals(deleted, DeleteIdentityProviderConfigResponse())
    }
  })

  private def assertIdentityProviderConfig(config: Option[IdentityProviderConfig])(
      f: IdentityProviderConfig => Unit
  ): Unit = {
    config match {
      case Some(value) => f(value)
      case None => fail("identity_provider_config expected")
    }
  }
}
