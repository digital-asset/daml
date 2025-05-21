// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors.executors

trait QueueAwareExecutor {

  /** Number of tasks that have not started execution yet
    */
  def queueSize: Long

}
