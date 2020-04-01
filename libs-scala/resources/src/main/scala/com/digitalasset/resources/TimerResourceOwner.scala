// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.Timer

import scala.concurrent.{ExecutionContext, Future}

class TimerResourceOwner(acquireTimer: () => Timer) extends ResourceOwner[Timer] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[Timer] =
    Resource(Future(acquireTimer()))(timer => Future(timer.cancel()))
}
