// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.api.util
import java.time.Duration

trait ToleranceWindow {

  def toleranceInPast: Duration
  def toleranceInFuture: Duration
}
