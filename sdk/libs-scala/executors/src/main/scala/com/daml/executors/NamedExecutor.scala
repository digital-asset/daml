// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors.executors

trait NamedExecutor {

  /** Unique name for the executor service that can be used as identifier in logs and metrics
    */
  def name: String

}
