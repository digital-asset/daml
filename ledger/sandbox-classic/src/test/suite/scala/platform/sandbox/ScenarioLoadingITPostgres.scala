// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.platform.sandbox.services.SandboxEnableAppendOnlySchema

final class ScenarioLoadingITPostgres extends ScenarioLoadingITBase with SandboxBackend.Postgresql

// TODO append-only: remove this class once the append-only schema is the default one
final class ScenarioLoadingITPostgresAppendOnly
    extends ScenarioLoadingITBase
    with SandboxBackend.Postgresql
    with SandboxEnableAppendOnlySchema
