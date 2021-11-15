// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset

case class WorkflowConfig(
    submission: Option[WorkflowConfig.SubmissionConfig] = None,
    streams: List[WorkflowConfig.StreamConfig] = Nil,
)

object WorkflowConfig {

  case class SubmissionConfig(
      numberOfInstances: Int,
      numberOfObservers: Int,
      uniqueParties: Boolean,
      instanceDistribution: List[WorkflowConfig.SubmissionConfig.ContractDescription],
  )

  object SubmissionConfig {
    case class ContractDescription(
        template: String,
        weight: Int,
        payloadSizeBytes: Int,
        archiveChance: Double,
    )
  }

  sealed trait StreamConfig {
    def name: String
  }

  object StreamConfig {
    final case class TransactionsStreamConfig(
        name: String,
        filters: List[PartyFilter],
        beginOffset: Option[LedgerOffset],
        endOffset: Option[LedgerOffset],
        objectives: StreamConfig.Objectives,
    ) extends StreamConfig

    final case class TransactionTreesStreamConfig(
        name: String,
        filters: List[PartyFilter],
        beginOffset: Option[LedgerOffset],
        endOffset: Option[LedgerOffset],
        objectives: StreamConfig.Objectives,
    ) extends StreamConfig

    final case class ActiveContractsStreamConfig(
        name: String,
        filters: List[PartyFilter],
    ) extends StreamConfig

    final case class CompletionsStreamConfig(
        name: String,
        party: String,
        applicationId: String,
        beginOffset: Option[LedgerOffset],
    ) extends StreamConfig

    final case class PartyFilter(party: String, templates: List[String])

    case class Objectives(
        maxDelaySeconds: Option[Long],
        minConsumptionSpeed: Option[Double],
    )
  }
}
