// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.reset

import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.services.{SandboxEnableAppendOnlySchema, SandboxFixture}

final class ResetServicePostgresqlIT
    extends ResetServiceDatabaseIT
    with SandboxFixture
    with SandboxBackend.Postgresql

// TODO append-only: remove this class once the append-only schema is the default one
final class ResetServicePostgresqlAppendOnlyIT
    extends ResetServiceDatabaseIT
    with SandboxFixture
    with SandboxBackend.Postgresql
    with SandboxEnableAppendOnlySchema
