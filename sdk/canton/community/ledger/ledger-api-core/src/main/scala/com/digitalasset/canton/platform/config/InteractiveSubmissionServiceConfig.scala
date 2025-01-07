// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

/** Configuration for the Ledger API Interactive Submission Service.
  *
  * @param enabled
  *        if false (default), the interactive submission service is disabled.
  * @param enableVerboseHashing
  *        if true, the prepare RPC will gather information about the hashing process of the transaction
  *        and return it as part of the response, if the "verboseHashing" flag was set on the request.
  *        If false (default), the "verboseHashing" flag on the prepare request has no effect.
  */
final case class InteractiveSubmissionServiceConfig(
    enabled: Boolean = false,
    enableVerboseHashing: Boolean = false,
)

object InteractiveSubmissionServiceConfig {
  lazy val Default: InteractiveSubmissionServiceConfig = InteractiveSubmissionServiceConfig()
}
