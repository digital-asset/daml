// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future

final class CreateIdentityProviderConfigAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {

  override def serviceCallName: String =
    "IdentityProviderConfigService#CreateIdentityProviderConfig"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    createConfig(context)

}
