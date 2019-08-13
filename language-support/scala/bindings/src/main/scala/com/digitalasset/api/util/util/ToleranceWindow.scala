// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.api.util
import java.time.Duration

trait ToleranceWindow {

  def toleranceInPast: Duration
  def toleranceInFuture: Duration
}
