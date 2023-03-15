// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.File

import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode

case class TestConfig(
    darPath: File,
    ledgerMode: LedgerMode,
    timeMode: ScriptTimeMode,
    maxInboundMessageSize: Int,
)
