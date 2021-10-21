// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.reset

import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.services.SandboxFixture

final class ResetServiceH2DatabaseIT
    extends ResetServiceDatabaseIT
    with SandboxFixture
    with SandboxBackend.H2Database
