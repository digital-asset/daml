// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

final case class SubmissionConfiguration(
    maxDeduplicationTime: Duration,
)

object SubmissionConfiguration {
  lazy val default: SubmissionConfiguration =
    SubmissionConfiguration(
      maxDeduplicationTime = Duration.ofDays(1),
    )
}
