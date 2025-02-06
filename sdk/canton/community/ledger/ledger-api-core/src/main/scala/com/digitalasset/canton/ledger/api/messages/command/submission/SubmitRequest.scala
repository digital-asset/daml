// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.submission

import com.digitalasset.canton.ledger.api.Commands

final case class SubmitRequest(commands: Commands)
