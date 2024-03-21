// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts.util

import com.daml.ledger.api.{v1 => lav1}

private[daml] object ClientUtil {
  def boxedRecord(a: lav1.value.Record): lav1.value.Value =
    lav1.value.Value(lav1.value.Value.Sum.Record(a))
}
