// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import com.daml.ledger.javaapi.data.Party

import java.time.Instant

sealed trait DriverStatus

object DriverStatus {

  final case class MasterStatus(
      timestamp: Instant,
      mode: String,
      proposalsPerSecond: Double,
      approvalsPerSecond: Double,
      totalProposals: Long,
      totalApprovals: Long,
  ) extends DriverStatus {

    override def toString: String =
      s"master(age=${Instant.now.toEpochMilli - timestamp.toEpochMilli}ms, mode=$mode, tps=${"%1.1f" format (proposalsPerSecond + approvalsPerSecond)}, total=${totalProposals + totalApprovals})"

  }

  final case class StepStatus(submitted: Int, observed: Int, open: Int) {
    override def toString: String =
      s"(s/o/q)=$submitted/$observed/$open"
  }

  final case class TraderStatus(
      name: String,
      timestamp: Instant,
      mode: String,
      currentRate: Double,
      maxRate: Double,
      latencyMs: Double,
      pending: Int,
      failed: Int,
      proposals: StepStatus,
      accepts: StepStatus,
      counterParties: Seq[(Party, Int, Int)],
      issuers: Seq[(Party, Int, Int)],
  ) extends DriverStatus {

    override def toString: String = s"""
         |trader(
         |name=$name,
         | report-age=${Instant.now.toEpochMilli - timestamp.toEpochMilli}ms,
         | mode=$mode,
         | rate(c/m)=(${"%1.1f" format currentRate}/${"%1.1f" format maxRate}),
         | latencyMs=${Math.round(latencyMs)},
         | pending=$pending,
         | proposals=$proposals,
         | accepts=$accepts,
         | failed=$failed)
         |""".stripMargin.replaceAll("\n", "")
  }

}
