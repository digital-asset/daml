// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.config.NonNegativeFiniteDuration

/** Configuration options for Canton admin workflows like `participant.health.ping`
  *
  * @param bongTestMaxLevel Upper bound on the level of a bong that the participant can initiate.
  *                         The participant can still participate in bongs at higher levels.
  *                         Use this to limit the load the participants can cause by initiating bongs.
  * @param retries The maximum number of times the [[com.digitalasset.canton.participant.ledger.api.client.CommandSubmitterWithRetry]]
  *                will retry an admin command submitted to the [[com.digitalasset.canton.participant.admin.AdminWorkflowServices]].
  * @param submissionTimeout Defines how long an admin workflow waits until it observes a completion for its command submissions
  *                          before it assumes that the submission is lost and retries.
  *                          If a lower timeout value is specified for an admin workflow, they take precedence.
  * @param autoLoadDar If set to true (default), we will load the admin workflow package automatically.
  *                    Setting this to false will break some admin workflows.
  */
final case class AdminWorkflowConfig(
    bongTestMaxLevel: Long = 0,
    retries: Int = 10,
    submissionTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(1),
    autoloadDar: Boolean = true,
)
