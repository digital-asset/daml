// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error._
import com.daml.error.definitions.{DamlError, DamlErrorWithDefiniteAnswer}

object IdentityProviderConfigServiceErrorGroup
    extends AdminServices.IdentityProviderConfigServiceErrorGroup {

  @Explanation(
    "There was an attempt to update an identity provider config using an invalid update request."
  )
  @Resolution(
    """|Inspect the error details for specific information on what made the request invalid.
       |Retry with an adjusted update request."""
  )
  object InvalidUpdateIdentityProviderConfigRequest
      extends ErrorCode(
        id = "INVALID_IDENTITY_PROVIDER_UPDATE_REQUEST",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    case class Reject(identityProviderId: String, reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause =
            s"Update operation for identity provider config '$identityProviderId' failed due to: $reason"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.IdentityProviderConfig -> identityProviderId
      )
    }
  }

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

  @Explanation("The identity provider config referred to by the request was not found.")
  @Resolution(
    "Check that you are connecting to the right participant node and the identity provider config is spelled correctly, or create the configuration."
  )
  object IdentityProviderConfigByIssuerNotFound
      extends ErrorCode(
        id = "IDP_CONFIG_BY_ISSUER_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Reject(operation: String, issuer: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed for unknown identity provider issuer=\"${issuer}\""
        )
  }

  @Explanation(
    "There already exists an identity provider configuration with the same identity provider id."
  )
  @Resolution(
    "Check that you are connecting to the right participant node and the identity provider id is spelled correctly, or use an identity provider that already exists."
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
    "There already exists an identity provider configuration with the same issuer."
  )
  @Resolution(
    "Check that you are connecting to the right participant node and the identity provider id is spelled correctly, or use an identity provider that already exists."
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

  @Explanation(
    """|A system can have only a limited number of identity provider configurations.
       |There was an attempt to create an identity provider configuration."""
  )
  @Resolution(
    """|Delete some of the already existing identity provider configurations.
       |Contact the participant operator if the limit is too low."""
  )
  object TooManyIdentityProviderConfigs
      extends ErrorCode(
        id = "TOO_MANY_IDENTITY_PROVIDER_CONFIGS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject(operation: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed."
        ) {}
  }
}
