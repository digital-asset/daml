// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.DamlErrorWithDefiniteAnswer
import com.daml.error.{
  ContextualizedErrorLogger,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  Explanation,
  Resolution,
}

object IdentityProviderConfigServiceErrorGroup
    extends AdminServices.IdentityProviderConfigServiceErrorGroup {

  @Explanation("The identity provider config referred to by the request was not found.")
  @Resolution(
    "Check that you are connecting to the right participant node and the identity provider config is spelled correctly, or create the configuration."
  )
  object IdentityProviderConfigNotFound
      extends ErrorCode(
        id = "IDP_CONFIG_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Reject(operation: String, identityProviderId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed for unknown identity provider id=\"${identityProviderId}\""
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.IdentityProviderConfig -> identityProviderId
      )
    }
  }
}
