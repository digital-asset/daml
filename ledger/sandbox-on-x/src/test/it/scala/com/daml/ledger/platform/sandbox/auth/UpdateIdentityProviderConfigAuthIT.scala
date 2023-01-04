// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.identity_provider_config_service.UpdateIdentityProviderConfigRequest
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.Future

final class UpdateIdentityProviderConfigAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {

  override def serviceCallName: String =
    "IdentityProviderConfigService#UpdateIdentityProviderConfig"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    for {
      response <- createConfig(context)
      _ <- idpStub(context).updateIdentityProviderConfig(
        UpdateIdentityProviderConfigRequest(
          identityProviderConfig = response.identityProviderConfig,
          updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
        )
      )
    } yield ()

}
