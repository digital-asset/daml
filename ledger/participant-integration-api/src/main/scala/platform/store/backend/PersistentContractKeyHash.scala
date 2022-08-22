// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.transaction.GlobalKey
import com.google.common.primitives.Longs

object PersistentContractKeyHash {

  def apply(key: GlobalKey): Long =
    Longs.fromByteArray(Array(0.toByte) ++ key.hash.bytes.toByteArray.take(7))

}
