// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import scala.concurrent.duration.*

final case class WatchdogConfig(
    enabled: Boolean,
    checkInterval: PositiveFiniteDuration = PositiveFiniteDuration(15.seconds),
    killDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration(30.seconds),
)
