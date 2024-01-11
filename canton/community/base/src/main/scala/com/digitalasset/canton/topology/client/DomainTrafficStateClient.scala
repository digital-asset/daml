// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.topology.Member

import scala.concurrent.Future

final case class MemberTrafficControlState(totalExtraTrafficLimit: PositiveLong)

/** The subset of the topology client providing traffic state information */
trait DomainTrafficControlStateClient {
  this: BaseTopologySnapshotClient =>

  /** Return the traffic control states for the members specified
    * @param members for which to return the traffic state
    * @return all input members with their optional traffic state
    */
  def trafficControlStatus(
      members: Seq[Member]
  ): Future[Map[Member, Option[MemberTrafficControlState]]]
}
