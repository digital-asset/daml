// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

/** Configuration of the Ledger API Command Submission Service
  * @param enableDeduplication
  *        Specifies whether the participant-side command deduplication should be turned on
  *        when available. By default, the command deduplication is turned on. However, on
  *        ledgers where the deduplication is implemented by the committer this parameter
  *        has no impact.
  */
case class SubmissionConfiguration(
    enableDeduplication: Boolean
)

object SubmissionConfiguration {

  lazy val default: SubmissionConfiguration =
    SubmissionConfiguration(
      enableDeduplication = true
    )
}
