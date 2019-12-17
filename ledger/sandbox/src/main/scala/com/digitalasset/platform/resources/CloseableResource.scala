// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.io.Closeable

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class CloseableResource[A](resource: Resource[A], releaseTimeout: FiniteDuration)
    extends Closeable {
  override def close(): Unit =
    Await.result(resource.release(), releaseTimeout)
}
