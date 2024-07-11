// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.workflows.java

object PackageID {
  val PingPong: String = pingpong.Ping.PACKAGE_ID
  val PingPongVacuum: String = pingpongvacuum.PingCleanup.PACKAGE_ID
  val DarDistribution: String = dardistribution.AcceptedDar.PACKAGE_ID
}
