// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.logging.LoggingContext

trait WriteSetSelector[ReadSet, WriteSet] {

  def selectWriteSet(
      preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet]
  )(implicit loggingContext: LoggingContext): WriteSet

}
