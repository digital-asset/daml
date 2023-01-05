// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import java.util.UUID

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.SubmissionId

trait SubmissionIdGenerator {
  def generate(): Ref.SubmissionId
}

object SubmissionIdGenerator {
  object Random extends SubmissionIdGenerator {
    override def generate(): SubmissionId =
      Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)
  }
}
