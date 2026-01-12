// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.TestHash

object TestUpdateId {
  def apply(s: String): UpdateId = UpdateId(TestHash.digest(s))
}
