// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.{
  ConsumingExercises,
  NonconsumingExercises,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset

case class WorkflowConfig(
    submission: Option[WorkflowConfig.SubmissionConfig] = None,
    streams: List[WorkflowConfig.StreamConfig] = Nil,
)

object WorkflowConfig {

  sealed trait SubmissionConfig extends Product with Serializable {
    def numberOfInstances: Int
    def numberOfObservers: Int
    def numberOfDivulgees: Int
    def numberOfExtraSubmitters: Int
    def uniqueParties: Boolean
  }

  final case class FibonacciSubmissionConfig(
      numberOfInstances: Int,
      uniqueParties: Boolean,
      value: Int,
  ) extends SubmissionConfig {
    override val numberOfObservers = 0
    override val numberOfDivulgees = 0
    override val numberOfExtraSubmitters = 0
  }

  final case class FooSubmissionConfig(
      numberOfInstances: Int,
      numberOfObservers: Int,
      numberOfDivulgees: Int,
      numberOfExtraSubmitters: Int,
      uniqueParties: Boolean,
      instanceDistribution: List[FooSubmissionConfig.ContractDescription],
      nonConsumingExercises: Option[NonconsumingExercises],
      consumingExercises: Option[ConsumingExercises],
      applicationIds: List[FooSubmissionConfig.ApplicationId],
  ) extends SubmissionConfig

  object FooSubmissionConfig {
    case class ContractDescription(
        template: String,
        weight: Int,
        payloadSizeBytes: Int,
    )

    case class NonconsumingExercises(
        probability: Double,
        payloadSizeBytes: Int,
    )

    case class ConsumingExercises(
        probability: Double,
        payloadSizeBytes: Int,
    )

    final case class ApplicationId(
        applicationId: String,
        weight: Int,
    )

  }

  sealed trait StreamConfig extends Product with Serializable {
    def name: String
  }

  object StreamConfig {
    final case class TransactionsStreamConfig(
        name: String,
        filters: List[PartyFilter],
        beginOffset: Option[LedgerOffset],
        endOffset: Option[LedgerOffset],
        objectives: Option[StreamConfig.TransactionObjectives],
    ) extends StreamConfig

    final case class TransactionTreesStreamConfig(
        name: String,
        filters: List[PartyFilter],
        beginOffset: Option[LedgerOffset],
        endOffset: Option[LedgerOffset],
        objectives: Option[StreamConfig.TransactionObjectives],
    ) extends StreamConfig

    final case class ActiveContractsStreamConfig(
        name: String,
        filters: List[PartyFilter],
        objectives: Option[StreamConfig.RateObjectives],
    ) extends StreamConfig

    final case class CompletionsStreamConfig(
        name: String,
        parties: List[String],
        applicationId: String,
        beginOffset: Option[LedgerOffset],
        objectives: Option[StreamConfig.RateObjectives],
    ) extends StreamConfig

    final case class PartyFilter(party: String, templates: List[String])

    case class TransactionObjectives(
        maxDelaySeconds: Option[Long],
        minConsumptionSpeed: Option[Double],
        minItemRate: Option[Double],
        maxItemRate: Option[Double],
    )

    case class RateObjectives(
        minItemRate: Option[Double],
        maxItemRate: Option[Double],
    )
  }
}
