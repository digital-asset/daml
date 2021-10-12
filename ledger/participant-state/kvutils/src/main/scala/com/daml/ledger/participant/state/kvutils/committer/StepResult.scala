// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.store.DamlLogEntry

private[kvutils] sealed trait StepResult[+PartialResult] {
  def map[NewPartialResult](f: PartialResult => NewPartialResult): StepResult[NewPartialResult]
  def flatMap[NewPartialResult](
      f: PartialResult => StepResult[NewPartialResult]
  ): StepResult[NewPartialResult]
}

private[kvutils] final case class StepContinue[PartialResult](partialResult: PartialResult)
    extends StepResult[PartialResult] {
  override def map[NewPartialResult](
      f: PartialResult => NewPartialResult
  ): StepResult[NewPartialResult] = StepContinue(f(partialResult))

  override def flatMap[NewPartialResult](
      f: PartialResult => StepResult[NewPartialResult]
  ): StepResult[NewPartialResult] = f(partialResult)
}

private[kvutils] final case class StepStop(logEntry: DamlLogEntry) extends StepResult[Nothing] {
  override def map[NewPartialResult](f: Nothing => NewPartialResult): StepResult[NewPartialResult] =
    this

  override def flatMap[NewPartialResult](
      f: Nothing => StepResult[NewPartialResult]
  ): StepResult[NewPartialResult] = this
}
