// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.workflows.java

object PackageID {
  val PingPong: String = pingpong.Ping.TEMPLATE_ID.getPackageId
  val PingPongVacuum: String = pingpongvacuum.PingCleanup.TEMPLATE_ID.getPackageId
  val DarDistribution: String = dardistribution.AcceptedDar.TEMPLATE_ID.getPackageId
}
