// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error._
import com.daml.error.definitions.DamlErrorWithDefiniteAnswer

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

  @Explanation(
    "There already exists a identity provider configuration with the same identity provider id."
  )
  @Resolution(
    "Check that you are connecting to the right participant node and the identity provider id is spelled correctly, or use the identity provider that already exists."
  )
  object IdentityProviderConfigAlreadyExists
      extends ErrorCode(
        id = "IDP_CONFIG_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    case class Reject(operation: String, identityProviderId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"${operation} failed, as identity provider \"${identityProviderId}\" already exists"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.IdentityProviderConfig -> identityProviderId
      )
    }
  }

  @Explanation(
    "There already exists a identity provider configuration with the same issuer."
  )
  @Resolution(
    "Check that you are connecting to the right participant node and the identity provider id is spelled correctly, or use the identity provider that already exists."
  )
  object IdentityProviderConfigIssuerAlreadyExists
      extends ErrorCode(
        id = "IDP_CONFIG_ISSUER_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    case class Reject(operation: String, issuer: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"${operation} failed, as identity provider with issuer \"${issuer}\" already exists"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.IdentityProviderConfig -> issuer
      )
    }
  }
}
