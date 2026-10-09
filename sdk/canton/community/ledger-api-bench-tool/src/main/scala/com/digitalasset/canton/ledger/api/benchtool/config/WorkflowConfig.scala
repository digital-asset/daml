// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.config

import scala.concurrent.duration.FiniteDuration

import WorkflowConfig.FooSubmissionConfig.{ConsumingExercises, NonconsumingExercises}
import WorkflowConfig.StreamConfig.PartyNamePrefixFilter

final case class WorkflowConfig(
    submission: Option[WorkflowConfig.SubmissionConfig] = None,
    streams: List[WorkflowConfig.StreamConfig] = Nil,
    pruning: Option[WorkflowConfig.PruningConfig] = None,
)

object WorkflowConfig {

  sealed trait SubmissionConfig extends Product with Serializable {
    def numberOfInstances: Int
    def numberOfObservers: Int
    def numberOfDivulgees: Int
    def numberOfExtraSubmitters: Int
    def uniqueParties: Boolean
    def waitForSubmission: Boolean
    def observerPartySets: List[FooSubmissionConfig.PartySet]
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
    override val observerPartySets: List[FooSubmissionConfig.PartySet] = List.empty
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
      userIds: List[FooSubmissionConfig.UserId] = List.empty,
      maybeWaitForSubmission: Option[Boolean] = None,
      observerPartySets: List[FooSubmissionConfig.PartySet] = List.empty,
      allowNonTransientContracts: Boolean = false,
  ) extends SubmissionConfig {
    def waitForSubmission: Boolean = maybeWaitForSubmission.getOrElse(true)
  }

  object FooSubmissionConfig {

    /** @param partyNamePrefix
      *   prefix of each party in this party set; also serves as its identifier
      * @param count
      *   number of parties to create
      * @param visibility
      *   a fraction of contracts that each of the parties from this set should see
      */
    final case class PartySet(
        partyNamePrefix: String,
        count: Int,
        visibility: Double,
    )

    final case class ContractDescription(
        template: String,
        weight: Int,
        payloadSizeBytes: Int,
    )

    final case class NonconsumingExercises(
        probability: Double,
        payloadSizeBytes: Int,
    )

    final case class ConsumingExercises(
        probability: Double,
        payloadSizeBytes: Int,
    )

    final case class UserId(
        userId: String,
        weight: Int,
    )

  }

  final case class PruningConfig(
      name: String,
      pruneAllDivulgedContracts: Boolean,
      maxDurationObjective: FiniteDuration,
  )

  sealed trait StreamConfig extends Product with Serializable {
    def name: String

    /** If specified, used to cancel the stream when enough items has been seen.
      */
    def maxItemCount: Option[Long] = None

    /** If specified, used to cancel the stream after the specified time out
      */
    def timeoutO: Option[FiniteDuration] = None

    def partySetPrefixes: List[String]

    def partyNamePrefixFilters: List[PartyNamePrefixFilter]

    def subscriptionDelay: Option[FiniteDuration]
  }

  object StreamConfig {

    final case class PartyFilter(
        party: String,
        templates: List[String] = List.empty,
        interfaces: List[String] = List.empty,
    )

    final case class PartyNamePrefixFilter(
        partyNamePrefix: String,
        templates: List[String] = List.empty,
        interfaces: List[String] = List.empty,
    )

    final case class TransactionsStreamConfig(
        name: String,
        filters: List[PartyFilter] = List.empty,
        partyNamePrefixFilters: List[PartyNamePrefixFilter] = List.empty,
        beginOffsetExclusive: Long = 0L,
        endOffsetInclusive: Option[Long] = None,
        objectives: Option[StreamConfig.TransactionObjectives] = None,
        subscriptionDelay: Option[FiniteDuration] = None,
        override val maxItemCount: Option[Long] = None,
        override val timeoutO: Option[FiniteDuration] = None,
    ) extends StreamConfig {
      override def partySetPrefixes: List[String] = partyNamePrefixFilters.map(_.partyNamePrefix)
    }

    final case class TransactionLedgerEffectsStreamConfig(
        name: String,
        filters: List[PartyFilter],
        partyNamePrefixFilters: List[PartyNamePrefixFilter] = List.empty,
        beginOffsetExclusive: Long = 0L,
        endOffsetInclusive: Option[Long] = None,
        objectives: Option[StreamConfig.TransactionObjectives] = None,
        subscriptionDelay: Option[FiniteDuration] = None,
        override val maxItemCount: Option[Long] = None,
        override val timeoutO: Option[FiniteDuration] = None,
    ) extends StreamConfig {
      override def partySetPrefixes: List[String] =
        partyNamePrefixFilters.map(_.partyNamePrefix)
    }

    final case class ActiveContractsStreamConfig(
        name: String,
        filters: List[PartyFilter],
        partyNamePrefixFilters: List[PartyNamePrefixFilter] = List.empty,
        objectives: Option[StreamConfig.AcsAndCompletionsObjectives] = None,
        subscriptionDelay: Option[FiniteDuration] = None,
        override val maxItemCount: Option[Long] = None,
        override val timeoutO: Option[FiniteDuration] = None,
    ) extends StreamConfig {
      override def partySetPrefixes: List[String] =
        partyNamePrefixFilters.map(_.partyNamePrefix)
    }

    final case class CompletionsStreamConfig(
        name: String,
        parties: List[String],
        userId: String,
        beginOffsetExclusive: Option[Long],
        objectives: Option[StreamConfig.AcsAndCompletionsObjectives],
        subscriptionDelay: Option[FiniteDuration] = None,
        override val maxItemCount: Option[Long],
        override val timeoutO: Option[FiniteDuration],
    ) extends StreamConfig {
      override def partySetPrefixes: List[String] = List.empty
      override def partyNamePrefixFilters: List[PartyNamePrefixFilter] = List.empty
    }

    trait CommonObjectivesConfig {
      def maxTotalStreamRuntimeDuration: Option[FiniteDuration]
      def minItemRate: Option[Double]
      def maxItemRate: Option[Double]
    }
    final case class TransactionObjectives(
        maxDelaySeconds: Option[Long],
        minConsumptionSpeed: Option[Double],
        override val minItemRate: Option[Double],
        override val maxItemRate: Option[Double],
        override val maxTotalStreamRuntimeDuration: Option[FiniteDuration] = None,
    ) extends CommonObjectivesConfig

    final case class AcsAndCompletionsObjectives(
        override val minItemRate: Option[Double],
        override val maxItemRate: Option[Double],
        override val maxTotalStreamRuntimeDuration: Option[FiniteDuration] = None,
    ) extends CommonObjectivesConfig
  }
}
