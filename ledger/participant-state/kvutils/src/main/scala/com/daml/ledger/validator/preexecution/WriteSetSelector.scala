// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

/** Selects a write set from a [[PreExecutionOutput]]. */
trait WriteSetSelector[ReadSet, WriteSet] {

  def selectWriteSet(preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet]): WriteSet

}
