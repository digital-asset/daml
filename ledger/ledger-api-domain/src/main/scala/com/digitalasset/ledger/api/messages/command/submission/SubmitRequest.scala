// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.command.submission

import brave.propagation.TraceContext
import com.digitalasset.ledger.api.domain

case class SubmitRequest(commands: domain.Commands, traceContext: Option[TraceContext])
