// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.identity_provider_config_service.UpdateIdentityProviderConfigRequest
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.google.protobuf.field_mask.FieldMask
import io.grpc.Status.Code

import scala.concurrent.Future

final class UpdateIdentityProviderConfigAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String =
    "IdentityProviderConfigService#UpdateIdentityProviderConfig"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    for {
      idpConfig <- createConfig(context)
      _ <- idpStub(context).updateIdentityProviderConfig(
        UpdateIdentityProviderConfigRequest(
          identityProviderConfig = Some(idpConfig),
          updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
        )
      )
    } yield ()
  }

  serviceCallName should {
    "deny updating an IDP config with issuer that already exists in another IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Update IDP Config that shares issuer with another IDP Config"
        )
      ) in { implicit env =>
      import env.*
      expectFailure(
        for {
          idpConfig1 <- createConfig(canBeAnAdmin)
          idpConfig2 <- createConfig(canBeAnAdmin)
          _ <- idpStub(canBeAnAdmin).updateIdentityProviderConfig(
            UpdateIdentityProviderConfigRequest(
              identityProviderConfig = Some(idpConfig2.copy(issuer = idpConfig1.issuer)),
              updateMask = Some(FieldMask(scala.Seq("issuer"))),
            )
          )
        } yield (),
        Code.ALREADY_EXISTS,
      )
    }
  }
}
