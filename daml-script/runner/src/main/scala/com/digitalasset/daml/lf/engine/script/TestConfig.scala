// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.File

import com.daml.lf.engine.script.ScriptTimeMode

case class TestConfig(
    darPath: File,
    participantMode: ParticipantMode,
    timeMode: ScriptTimeMode,
    maxInboundMessageSize: Int,
)
