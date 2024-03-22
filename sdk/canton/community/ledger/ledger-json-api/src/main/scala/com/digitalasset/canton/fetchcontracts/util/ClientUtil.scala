// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts.util

import com.daml.ledger.api.{v2 => lav2}

 object ClientUtil {
  def boxedRecord(a: lav2.value.Record): lav2.value.Value =
    lav2.value.Value(lav2.value.Value.Sum.Record(a))
}
