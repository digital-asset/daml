// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

final case class LedgerApiObjectMeta(
    resourceVersion: String,
    annotations: Map[String, String],
)

object LedgerApiObjectMeta {
  def empty: LedgerApiObjectMeta =
    LedgerApiObjectMeta(resourceVersion = "", annotations = Map.empty)
}
