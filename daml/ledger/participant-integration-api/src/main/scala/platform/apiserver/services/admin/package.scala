// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.util.UUID

import com.daml.lf.data.Ref

package object admin {
  private[admin] def augmentSubmissionId(submissionId: String): Ref.SubmissionId = {
    val uuid = UUID.randomUUID().toString
    val raw = if (submissionId.isEmpty) uuid else s"$submissionId-$uuid"
    Ref.SubmissionId.assertFromString(raw)
  }
}
