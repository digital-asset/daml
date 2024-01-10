// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.ledger.api.v2.commands.Commands

import java.time.Duration

final case class CommandSubmission(commands: Commands, timeout: Option[Duration] = None)
