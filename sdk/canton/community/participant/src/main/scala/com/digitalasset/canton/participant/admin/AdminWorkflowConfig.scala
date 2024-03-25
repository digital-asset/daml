// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

/** Configuration options for Canton admin workflows like `participant.health.ping`
  *
  * @param bongTestMaxLevel Upper bound (exclusive) on the level of a bong that the participant can participate.
  *                         Any bong with higher level will be vacuumed immediately. Default 0, which means it won't
  *                         participate in any bongs.
  * @param pingResponseTimeout How long we will attempt to respond to a ping request before giving up
  * @param maxBongDuration Cap the maximum duration of a bong. Default is 15 minutes.
  * @param retries If false (default true), we will not retry sending commands in case of failures
  * @param autoLoadDar If set to true (default), we will load the admin workflow package automatically.
  *                    Setting this to false will break some admin workflows.
  */
final case class AdminWorkflowConfig(
    bongTestMaxLevel: NonNegativeInt = NonNegativeInt.tryCreate(0),
    pingResponseTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
    maxBongDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(15),
    retries: Boolean = true,
    autoLoadDar: Boolean = true,
)
