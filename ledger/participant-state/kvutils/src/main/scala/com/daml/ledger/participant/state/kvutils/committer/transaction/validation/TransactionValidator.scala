// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.committer.transaction.{Rejections, Step}

private[transaction] trait TransactionValidator {
  def createValidationStep(rejections: Rejections): Step
}
