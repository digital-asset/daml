// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

// TODO append-only: remove once the append-only schema is enabled by default
trait SandboxEnableAppendOnlySchema {
  this: SandboxFixture =>

  override def enableAppendOnlySchema: Boolean = true
}
