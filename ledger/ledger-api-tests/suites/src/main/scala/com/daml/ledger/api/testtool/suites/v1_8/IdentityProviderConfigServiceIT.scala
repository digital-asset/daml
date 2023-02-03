// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    "IdentityProviderCreateConfigInvalidArguments",
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
    ): Future[Unit] = {
      for {
        throwable <- ledger
          .createIdentityProviderConfig(
            identityProviderId,
            isDeactivated,
            issuer,
            jwksUrl,
          )
          .mustFail(context = problem)
      } yield assertGrpcError(
        t = throwable,
        errorCode = expectedErrorCode,
        exceptionMessageSubstring = None,
      )
    }

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
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
        LedgerApiErrors.RequestValidation.InvalidField,
        jwksUrl = "",
      )
      _ <- createAndCheck(
        "non valid url",
        LedgerApiErrors.RequestValidation.InvalidField,
        jwksUrl = "url.com",
      )
      createEmptyIdpConfig <- ledger
        .createIdentityProviderConfig(CreateIdentityProviderConfigRequest(None))
        .mustFail(context = "empty identity_provider_config")
    } yield {
      assertGrpcError(
        t = createEmptyIdpConfig,
        errorCode = LedgerApiErrors.RequestValidation.MissingField,
        exceptionMessageSubstring = None,
      )
    }
  })

  test(
    "IdentityProviderGetConfigInvalidArguments",
    "Test argument validation for IdentityProviderConfigService#GetIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def createAndCheck(
        problem: String,
        expectedErrorCode: ErrorCode,
        identityProviderId: String,
    ): Future[Unit] = {
      for {
        throwable <- ledger
          .getIdentityProviderConfig(
            GetIdentityProviderConfigRequest(identityProviderId)
          )
          .mustFail(context = problem)
      } yield assertGrpcError(
        t = throwable,
        errorCode = expectedErrorCode,
        exceptionMessageSubstring = None,
      )
    }

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
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
    "IdentityProviderUpdateConfigInvalidArguments",
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
    ): Future[Unit] = {
      for {
        throwable <- ledger
          .updateIdentityProviderConfig(
            identityProviderId,
            isDeactivated,
            issuer,
            jwksUrl,
            updateMask,
          )
          .mustFail(context = problem)
      } yield assertGrpcError(
        t = throwable,
        errorCode = expectedErrorCode,
        exceptionMessageSubstring = None,
      )
    }

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
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
      updateEmptyConfig <- ledger
        .updateIdentityProviderConfig(UpdateIdentityProviderConfigRequest(None))
        .mustFail(context = "empty identity_provider_config")

      createdIdp <- ledger.createIdentityProviderConfig()

      emptyUpdateMask <- ledger
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            Some(createdIdp.identityProviderConfig.get),
            Some(FieldMask(Seq.empty)),
          )
        )
        .mustFail(context = "empty update_mask")
    } yield {
      assertGrpcError(
        t = updateEmptyConfig,
        errorCode = LedgerApiErrors.RequestValidation.MissingField,
        exceptionMessageSubstring = None,
      )

      assertGrpcError(
        t = emptyUpdateMask,
        errorCode =
          LedgerApiErrors.Admin.IdentityProviderConfig.InvalidUpdateIdentityProviderConfigRequest,
        exceptionMessageSubstring = Some("update mask contains no entries"),
      )

    }
  })

  test(
    "IdentityProviderDeleteConfigInvalidArguments",
    "Test argument validation for IdentityProviderConfigService#DeleteIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def createAndCheck(
        problem: String,
        expectedErrorCode: ErrorCode,
        identityProviderId: String,
    ): Future[Unit] = {
      for {
        throwable <- ledger
          .deleteIdentityProviderConfig(
            DeleteIdentityProviderConfigRequest(identityProviderId)
          )
          .mustFail(context = problem)
      } yield assertGrpcError(
        t = throwable,
        errorCode = expectedErrorCode,
        exceptionMessageSubstring = None,
      )
    }

    for {
      _ <- createAndCheck(
        "empty identity_provider_id",
        LedgerApiErrors.RequestValidation.InvalidField,
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
    "IdentityProviderCreateConfig",
    "Exercise CreateIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val identityProviderId: String = UUID.randomUUID().toString
    val isDeactivated: Boolean = false
    val issuer: String = UUID.randomUUID().toString
    val jwksUrl: String = "http://daml.com/jwks.json"
    for {
      response1 <- ledger.createIdentityProviderConfig(
        identityProviderId,
        isDeactivated,
        issuer,
        jwksUrl,
      )
      response2 <- ledger.createIdentityProviderConfig(
        isDeactivated = true
      )
      sameIdpIdExists <- ledger
        .createIdentityProviderConfig(
          identityProviderId,
          isDeactivated,
          UUID.randomUUID().toString,
          jwksUrl,
        )
        .mustFail("Creating duplicate IDP with the same ID")

      sameIssuerExists <- ledger
        .createIdentityProviderConfig(
          issuer = issuer
        )
        .mustFail("Creating duplicate IDP with the same issuer")

    } yield {
      assertIdentityProviderConfig(response1.identityProviderConfig) { config =>
        assertEquals(config.identityProviderId, identityProviderId)
        assertEquals(config.isDeactivated, isDeactivated)
        assertEquals(config.issuer, issuer)
        assertEquals(config.jwksUrl, jwksUrl)
      }
      assertIdentityProviderConfig(response2.identityProviderConfig) { config =>
        assertEquals(config.isDeactivated, true)
      }
      assertGrpcError(
        t = sameIdpIdExists,
        errorCode =
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigAlreadyExists,
        exceptionMessageSubstring = None,
      )
      assertGrpcError(
        t = sameIssuerExists,
        errorCode =
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigIssuerAlreadyExists,
        exceptionMessageSubstring = None,
      )
    }
  })

  test(
    "IdentityProviderCreateConfig",
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
      duplicateIssuerError <- ledger
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              response3.identityProviderConfig.map(_.copy(issuer = duplicateIssuer)),
            updateMask = Some(FieldMask(Seq("issuer"))),
          )
        )
        .mustFail("Updating to the issuer which already exists")
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

      assertGrpcError(
        t = duplicateIssuerError,
        errorCode =
          LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigIssuerAlreadyExists,
        exceptionMessageSubstring = None,
      )
    }
  })

  test(
    "IdentityProviderCreateConfig",
    "Exercise GetIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val identityProviderId: String = UUID.randomUUID().toString
    val isDeactivated: Boolean = false
    val issuer: String = UUID.randomUUID().toString
    val jwksUrl: String = "http://daml.com/jwks.json"
    for {
      response1 <- ledger.createIdentityProviderConfig(
        identityProviderId,
        isDeactivated,
        issuer,
        jwksUrl,
      )
      response2 <- ledger.getIdentityProviderConfig(
        GetIdentityProviderConfigRequest(identityProviderId)
      )
      response3 <- ledger
        .getIdentityProviderConfig(
          GetIdentityProviderConfigRequest(
            UUID.randomUUID().toString
          )
        )
        .mustFail("non existing idp")
    } yield {
      assertIdentityProviderConfig(response1.identityProviderConfig) { config =>
        assertEquals(config.identityProviderId, identityProviderId)
        assertEquals(config.isDeactivated, isDeactivated)
        assertEquals(config.issuer, issuer)
        assertEquals(config.jwksUrl, jwksUrl)
      }
      assertIdentityProviderConfig(response2.identityProviderConfig) { config =>
        assertEquals(config.identityProviderId, identityProviderId)
        assertEquals(config.isDeactivated, isDeactivated)
        assertEquals(config.issuer, issuer)
        assertEquals(config.jwksUrl, jwksUrl)
      }
      assertGrpcError(
        t = response3,
        errorCode = LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigNotFound,
        exceptionMessageSubstring = None,
      )
    }
  })

  test(
    "IdentityProviderCreateConfig",
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
    "IdentityProviderCreateConfig",
    "Exercise DeleteIdentityProviderConfig",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val id = UUID.randomUUID().toString
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = id)
      deleted <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(id))
      configDoesNotExist <- ledger
        .deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(id))
        .mustFail("config does not exist anymore")
    } yield {
      assertEquals(deleted, DeleteIdentityProviderConfigResponse())
      assertGrpcError(
        t = configDoesNotExist,
        errorCode = LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigNotFound,
        exceptionMessageSubstring = None,
      )
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
