// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.digitalasset.daml.lf.data.Ref

import java.util.UUID

package object admin {
  private[admin] def augmentSubmissionId(submissionId: String): Ref.SubmissionId = {
    val uuid = UUID.randomUUID().toString
    val raw = if (submissionId.isEmpty) uuid else s"$submissionId-$uuid"
    Ref.SubmissionId.assertFromString(raw)
  }
}
