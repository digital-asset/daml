// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.api.checks.pkg

import java.util.UUID

object GenMissingString {
  def apply(collection: Set[String]): String = {
    var candidate = UUID.randomUUID().toString
    while (collection.contains(candidate)) {
      candidate = UUID.randomUUID().toString
    }
    candidate
  }

}
