// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.daml.ledger.api.v2.admin
import scalapb.GeneratedMessageCompanion

object FieldNames {
  object User {
    val id: String = resolveFieldName(admin.user_management_service.User)(_.ID_FIELD_NUMBER)
    val primaryParty: String =
      resolveFieldName(admin.user_management_service.User)(_.PRIMARY_PARTY_FIELD_NUMBER)
    val isDeactivated: String =
      resolveFieldName(admin.user_management_service.User)(_.IS_DEACTIVATED_FIELD_NUMBER)
    val metadata: String =
      resolveFieldName(admin.user_management_service.User)(_.METADATA_FIELD_NUMBER)
    val identityProviderId: String =
      resolveFieldName(admin.user_management_service.User)(_.IDENTITY_PROVIDER_ID_FIELD_NUMBER)
  }
  object Metadata {
    val annotations: String =
      resolveFieldName(admin.object_meta.ObjectMeta)(_.ANNOTATIONS_FIELD_NUMBER)
    val resourceVersion: String =
      resolveFieldName(admin.object_meta.ObjectMeta)(_.RESOURCE_VERSION_FIELD_NUMBER)
  }

  object PartyDetails {
    val party: String =
      resolveFieldName(admin.party_management_service.PartyDetails)(_.PARTY_FIELD_NUMBER)
    val localMetadata: String =
      resolveFieldName(admin.party_management_service.PartyDetails)(_.LOCAL_METADATA_FIELD_NUMBER)
    val displayName: String =
      resolveFieldName(admin.party_management_service.PartyDetails)(_.DISPLAY_NAME_FIELD_NUMBER)
    val isLocal: String =
      resolveFieldName(admin.party_management_service.PartyDetails)(_.IS_LOCAL_FIELD_NUMBER)
    val identityProviderId: String =
      resolveFieldName(admin.party_management_service.PartyDetails)(
        _.IDENTITY_PROVIDER_ID_FIELD_NUMBER
      )
  }

  object IdentityProviderConfig {
    val identityProviderId: String =
      resolveFieldName(admin.identity_provider_config_service.IdentityProviderConfig)(
        _.IDENTITY_PROVIDER_ID_FIELD_NUMBER
      )
    val issuer =
      resolveFieldName(admin.identity_provider_config_service.IdentityProviderConfig)(
        _.ISSUER_FIELD_NUMBER
      )
    val isDeactivated =
      resolveFieldName(admin.identity_provider_config_service.IdentityProviderConfig)(
        _.IS_DEACTIVATED_FIELD_NUMBER
      )
    val jwksUrl =
      resolveFieldName(admin.identity_provider_config_service.IdentityProviderConfig)(
        _.JWKS_URL_FIELD_NUMBER
      )
    val audience =
      resolveFieldName(admin.identity_provider_config_service.IdentityProviderConfig)(
        _.AUDIENCE_FIELD_NUMBER
      )
  }

  private def resolveFieldName[A <: GeneratedMessageCompanion[_]](
      companion: A
  )(getFieldNumberFun: A => Int): String = {
    val fieldNumber = getFieldNumberFun(companion)
    companion.scalaDescriptor
      .findFieldByNumber(fieldNumber)
      .getOrElse(sys.error(s"Unknown field number $fieldNumber on $companion"))
      .name
  }

}
