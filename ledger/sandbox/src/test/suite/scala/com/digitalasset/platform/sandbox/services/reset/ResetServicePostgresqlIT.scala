// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.reset

import com.digitalasset.platform.sandbox.SandboxBackend
import com.digitalasset.platform.sandbox.services.SandboxFixture

final class ResetServicePostgresqlIT
    extends ResetServiceDatabaseIT
    with SandboxFixture
    with SandboxBackend.Postgresql
