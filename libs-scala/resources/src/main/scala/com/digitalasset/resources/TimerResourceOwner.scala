// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.Timer

import scala.concurrent.{ExecutionContext, Future}

class TimerResourceOwner(acquireTimer: () => Timer) extends ResourceOwner[Timer] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[Timer] =
    Resource(Future(acquireTimer()))(timer => Future(timer.cancel()))
}
