// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.Timer

import scala.concurrent.Future

class TimerResourceOwner[Context: HasExecutionContext](acquireTimer: () => Timer)
    extends AbstractResourceOwner[Context, Timer] {
  override def acquire()(implicit context: Context): Resource[Context, Timer] =
    ReleasableResource(Future(acquireTimer()))(timer => Future(timer.cancel()))
}
