// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import io.grpc.Status.Code

import scala.concurrent.Future

final class CreateIdentityProviderConfigAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String =
    "IdentityProviderConfigService#CreateIdentityProviderConfig"

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    createConfig(context)(env.executionContext)

  serviceCallName should {
    "deny creating an IDP config with issuer that already exists in another IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Create IDP Config that shares issuer with another IDP Config"
        )
      ) in { implicit env =>
      import env.*
      expectFailure(
        for {
          idpConfig1 <- createConfig(canBeAnAdmin)
          _ <- createConfig(canBeAnAdmin, issuer = Some(idpConfig1.issuer))
        } yield (),
        Code.ALREADY_EXISTS,
      )
    }
  }

}
