// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

package js {

  import io.circe.Json

  final case class PrefetchContractKey(
      templateId: com.daml.ledger.api.v2.value.Identifier,
      contractKey: Json,
  )

}
