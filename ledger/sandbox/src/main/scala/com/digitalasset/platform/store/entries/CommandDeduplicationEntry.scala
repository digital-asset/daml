// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.entries

import java.time.Instant

final case class CommandDeduplicationEntry(
    deduplicationKey: String,
    ttl: Instant,
)
