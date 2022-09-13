// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import scalapb.GeneratedMessageCompanion
import com.daml.ledger.api.v1.admin

object FieldNames {
  object UpdateUserRequest {
    val user: String =
      resolveFieldName(admin.user_management_service.UpdateUserRequest)(_.USER_FIELD_NUMBER)
  }
  object User {
    val primaryParty: String =
      resolveFieldName(admin.user_management_service.User)(_.PRIMARY_PARTY_FIELD_NUMBER)
    val isDeactivated: String =
      resolveFieldName(admin.user_management_service.User)(_.IS_DEACTIVATED_FIELD_NUMBER)
    val metadata: String =
      resolveFieldName(admin.user_management_service.User)(_.METADATA_FIELD_NUMBER)
  }
  object Metadata {
    val annotations: String =
      resolveFieldName(admin.object_meta.ObjectMeta)(_.ANNOTATIONS_FIELD_NUMBER)
  }

  object UpdatePartyDetailsRequest {
    val partyDetails: String = resolveFieldName(
      admin.party_management_service.UpdatePartyDetailsRequest
    )(_.PARTY_DETAILS_FIELD_NUMBER)
  }
  object PartyDetails {
    val localMetadata: String =
      resolveFieldName(admin.party_management_service.PartyDetails)(_.LOCAL_METADATA_FIELD_NUMBER)
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
