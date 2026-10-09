// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

package js {

  import io.circe.Json

  final case class AllocatePartyRequest(
      partyIdHint: String,
      localMetadata: Option[com.daml.ledger.api.v2.admin.object_meta.ObjectMeta] = None,
      identityProviderId: String = "",
      synchronizerId: String = "",
      userId: String = "",
  )

  final case class PrefetchContractKey(
      templateId: Option[com.daml.ledger.api.v2.value.Identifier],
      contractKey: Json,
  )

}
