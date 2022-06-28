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
    def waitForSubmission: Boolean
  }

  final case class FibonacciSubmissionConfig(
      numberOfInstances: Int,
      uniqueParties: Boolean,
      value: Int,
      waitForSubmission: Boolean,
  ) extends SubmissionConfig {
    override val numberOfObservers = 0
    override val numberOfDivulgees = 0
    override val numberOfExtraSubmitters = 0
  }

  final case class FooSubmissionConfig(
      numberOfInstances: Int,
      numberOfObservers: Int,
      uniqueParties: Boolean,
      instanceDistribution: List[FooSubmissionConfig.ContractDescription],
      numberOfDivulgees: Int = 0,
      numberOfExtraSubmitters: Int = 0,
      nonConsumingExercises: Option[NonconsumingExercises] = None,
      consumingExercises: Option[ConsumingExercises] = None,
      applicationIds: List[FooSubmissionConfig.ApplicationId] = List.empty,
      maybeWaitForSubmission: Option[Boolean] = None,
  ) extends SubmissionConfig {
    def waitForSubmission: Boolean = maybeWaitForSubmission.getOrElse(true)
  }

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

    /** If specified, used to cancel the stream when enough items has been seen.
      */
    def maxItemCount: Option[Long] = None

    /** If specified, used to cancel the stream after the specified time out
      */
    def timeoutInSecondsO: Option[Long] = None
  }

  object StreamConfig {

    final case class PartyFilter(party: String, templates: List[String] = List.empty)

    final case class TransactionsStreamConfig(
        name: String,
        filters: List[PartyFilter],
        beginOffset: Option[LedgerOffset] = None,
        endOffset: Option[LedgerOffset] = None,
        objectives: Option[StreamConfig.TransactionObjectives] = None,
        override val maxItemCount: Option[Long] = None,
        override val timeoutInSecondsO: Option[Long] = None,
    ) extends StreamConfig

    final case class TransactionTreesStreamConfig(
        name: String,
        filters: List[PartyFilter],
        beginOffset: Option[LedgerOffset] = None,
        endOffset: Option[LedgerOffset] = None,
        objectives: Option[StreamConfig.TransactionObjectives] = None,
        override val maxItemCount: Option[Long] = None,
        override val timeoutInSecondsO: Option[Long] = None,
    ) extends StreamConfig

    final case class ActiveContractsStreamConfig(
        name: String,
        filters: List[PartyFilter],
        objectives: Option[StreamConfig.RateObjectives] = None,
        override val maxItemCount: Option[Long] = None,
        override val timeoutInSecondsO: Option[Long] = None,
    ) extends StreamConfig

    final case class CompletionsStreamConfig(
        name: String,
        parties: List[String],
        applicationId: String,
        beginOffset: Option[LedgerOffset],
        objectives: Option[StreamConfig.RateObjectives],
        override val maxItemCount: Option[Long],
        override val timeoutInSecondsO: Option[Long],
    ) extends StreamConfig

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
