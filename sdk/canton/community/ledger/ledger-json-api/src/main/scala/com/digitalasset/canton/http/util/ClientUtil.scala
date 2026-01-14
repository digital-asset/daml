// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import java.util.UUID

object ClientUtil {
  def uniqueId(): String = UUID.randomUUID.toString
}
