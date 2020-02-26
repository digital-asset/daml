// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.configuration

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class SubmissionConfiguration(
    maxTtl: FiniteDuration,
)

object SubmissionConfiguration {
  lazy val default =
    SubmissionConfiguration(
      maxTtl = 1.hours,
    )
}
