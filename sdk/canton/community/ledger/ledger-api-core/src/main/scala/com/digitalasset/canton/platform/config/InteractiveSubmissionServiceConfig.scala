// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.RequireTypes.PositiveInt

/** Configuration for the Ledger API Interactive Submission Service.
  *
  * @param enableVerboseHashing
  *   if true, the prepare RPC will gather information about the hashing process of the transaction
  *   and return it as part of the response, if the "verboseHashing" flag was set on the request. If
  *   false (default), the "verboseHashing" flag on the prepare request has no effect.
  * @param contractLookupParallelism
  *   When the transaction uses input contracts, the preparing participant will attempt to look them
  *   up from its local store if they are not explicitly disclosed. This limits the parallelism at
  *   which this lookup will be done to avoid potentially overwhelming the database
  * @param enforceSingleRootNode
  *   Reject early requests with multiple commands (prepare) or multiple root nodes (execute). Such
  *   transactions are not supported and would be rejected anyway during confirmation. Disabling
  *   this check won't provide functional support for those transactions, the flag only rejects them
  *   early and prevent their submission to the synchronizer.
  */
final case class InteractiveSubmissionServiceConfig(
    enableVerboseHashing: Boolean = false,
    contractLookupParallelism: PositiveInt = PositiveInt.tryCreate(5),
    enforceSingleRootNode: Boolean = true,
)

object InteractiveSubmissionServiceConfig {
  lazy val Default: InteractiveSubmissionServiceConfig = InteractiveSubmissionServiceConfig()
}
