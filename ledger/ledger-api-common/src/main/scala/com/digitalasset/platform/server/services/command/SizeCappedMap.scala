// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.command

import java.util

private class SizeCappedMap[K, V](initialCapacity: Int, maxCapacity: Int)
    extends util.LinkedHashMap[K, V](initialCapacity) {

  override def removeEldestEntry(eldest: util.Map.Entry[K, V]): Boolean = {
    size() > maxCapacity
  }

}
