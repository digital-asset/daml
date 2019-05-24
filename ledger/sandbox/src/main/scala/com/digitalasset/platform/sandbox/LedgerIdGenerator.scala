// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.util.UUID

object LedgerIdGenerator {
  def generateRandomId(): String =
    s"sandbox-${UUID.randomUUID().toString}"
}
