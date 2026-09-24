// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt, PositiveLong}
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member

trait TrafficBalanceSupport extends BaseTest {
  protected def getTrafficForMember(
      member: Member
  )(implicit env: TestConsoleEnvironment): Option[TrafficState] = {
    import env.*

    sequencer1.traffic_control
      .traffic_state_of_members_approximate(Seq(member))
      .trafficStates
      .get(member)
  }

  protected def updateBalanceForMember(
      instance: LocalInstanceReference,
      newBalance: PositiveLong,
      beforeCheck: () => Unit,
  )(implicit env: TestConsoleEnvironment) = {
    val member = instance.id.member

    sendTopUp(member, newBalance.toNonNegative)

    eventually() {
      beforeCheck()
      getTrafficForMember(member).value.extraTrafficPurchased.value shouldBe newBalance.value
    }
  }

  protected def sendTopUp(
      member: Member,
      newBalance: NonNegativeLong,
      serialO: Option[PositiveInt] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): PositiveInt = {
    import env.*

    val serial = serialO
      .orElse(getTrafficForMember(member).flatMap(_.serial).map(_.increment))
      .getOrElse(PositiveInt.one)

    sequencer1.traffic_control.set_traffic_balance(member, serial, newBalance)

    serial
  }
}
