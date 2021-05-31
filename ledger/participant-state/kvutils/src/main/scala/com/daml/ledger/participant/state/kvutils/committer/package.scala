// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

package object committer {
  private[committer] type StepInfo = String

  private[committer] type Steps[PartialResult] = Iterable[(StepInfo, CommitStep[PartialResult])]
}
