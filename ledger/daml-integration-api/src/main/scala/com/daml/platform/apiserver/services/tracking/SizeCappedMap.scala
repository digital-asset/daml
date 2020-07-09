// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.util

private[tracking] class SizeCappedMap[K, V](initialCapacity: Int, maxCapacity: Int)
    extends util.LinkedHashMap[K, V](initialCapacity) {

  override def removeEldestEntry(eldest: util.Map.Entry[K, V]): Boolean = {
    size() > maxCapacity
  }

}
