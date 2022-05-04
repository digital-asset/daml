// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.{
  EmptySubmissionConfig,
  FibonacciSubmissionConfig,
  FooSubmissionConfig,
  SubmissionConfig,
}
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.client.binding.Primitive

import scala.util.{Success, Try}

trait CommandGenerator {
  def next(): Try[Seq[Command]]
}

object CommandGenerator {
  def apply(
      randomnessProvider: RandomnessProvider,
      config: SubmissionConfig,
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
  ): CommandGenerator = config match {
    case c: FooSubmissionConfig =>
      new FooCommandGenerator(randomnessProvider, c, signatory, observers)
    case c: FibonacciSubmissionConfig =>
      new FibonacciCommandGenerator(c, signatory)
    case EmptySubmissionConfig =>
      () => Success(Seq.empty)
  }
}
