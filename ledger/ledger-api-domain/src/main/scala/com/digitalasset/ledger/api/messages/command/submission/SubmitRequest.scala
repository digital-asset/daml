// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.command.submission

import brave.propagation.TraceContext
import com.daml.ledger.api.domain

case class SubmitRequest(commands: domain.Commands, traceContext: Option[TraceContext])
