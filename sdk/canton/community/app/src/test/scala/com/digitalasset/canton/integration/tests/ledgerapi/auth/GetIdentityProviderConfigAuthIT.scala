// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetIdentityProviderConfigAuthIT extends AdminServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String =
    "IdentityProviderConfigService#GetIdentityProviderConfig"

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    for {
      idpConfig <- createConfig(canBeAnAdmin)
      _ <- getConfig(context, idpConfig.identityProviderId)
    } yield ()
  }

}
