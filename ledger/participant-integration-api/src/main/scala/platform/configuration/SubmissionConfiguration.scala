// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

/** Configuration of the Ledger API Command Submission Service
  * @param enableDeduplication
  *        Specifies whether the participant deduplication can be turned on or off.
  *        By default, deduplication is turned on. There is a class of ledgers where the
  *        deduplication is implemented by the committer. On those ledgers this parameter
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
