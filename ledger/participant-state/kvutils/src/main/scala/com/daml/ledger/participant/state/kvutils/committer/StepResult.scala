// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry

private[kvutils] sealed trait StepResult[+PartialResult]
private[kvutils] final case class StepContinue[PartialResult](partialResult: PartialResult)
    extends StepResult[PartialResult]
private[kvutils] final case class StepStop(logEntry: DamlLogEntry) extends StepResult[Nothing]
