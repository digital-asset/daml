// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.ExecutionContext

trait ResourceOwner[T] {
  def acquire()(implicit executionContext: ExecutionContext): Resource[T]
}
