// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

/** Configuration for the Ledger API Interactive Submission Service.
  *
  * @param enabled
  *        if false (default), the interactive submission service is disabled.
  */
final case class InteractiveSubmissionServiceConfig(
    enabled: Boolean = false
)

object InteractiveSubmissionServiceConfig {
  lazy val Default: InteractiveSubmissionServiceConfig = InteractiveSubmissionServiceConfig()
}
