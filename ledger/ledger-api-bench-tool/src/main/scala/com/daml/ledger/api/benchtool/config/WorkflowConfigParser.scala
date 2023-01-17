// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import cats.syntax.functor._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import io.circe.yaml.parser
import io.circe.{Decoder, HCursor}
import java.io.Reader

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.{
  ConsumingExercises,
  NonconsumingExercises,
}
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  PartyFilter,
  PartyNamePrefixFilter,
}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

object WorkflowConfigParser {
  import Decoders._
  import WorkflowConfig._

  def parse(reader: Reader): Either[ParserError, WorkflowConfig] =
    parser
      .parse(reader)
      .flatMap(_.as[WorkflowConfig])
      .left
      .map(error => ParserError(error.getLocalizedMessage))

  case class ParserError(details: String)

  object Decoders {
    implicit val scalaDurationDecoder: Decoder[FiniteDuration] =
      Decoder.decodeString.emapTry(strDuration =>
        Try(Duration(strDuration)).flatMap {
          case infinite: Duration.Infinite =>
            Failure(
              new IllegalArgumentException(
                s"Subscription delay duration must be finite, but got $infinite"
              )
            )
          case duration: FiniteDuration => Success(duration)
        }
      )

    implicit val transactionObjectivesDecoder: Decoder[StreamConfig.TransactionObjectives] =
      Decoder.forProduct5(
        "max_delay_seconds",
        "min_consumption_speed",
        "min_item_rate",
        "max_item_rate",
        "max_stream_duration",
      )(StreamConfig.TransactionObjectives.apply)

    implicit val rateObjectivesDecoder: Decoder[StreamConfig.AcsAndCompletionsObjectives] =
      Decoder.forProduct3(
        "min_item_rate",
        "max_item_rate",
        "max_stream_duration",
      )(StreamConfig.AcsAndCompletionsObjectives.apply)

    implicit val offsetDecoder: Decoder[LedgerOffset] = {
      Decoder.decodeString.map {
        case "ledger-begin" => LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
        case "ledger-end" => LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)
        case absolute => LedgerOffset.defaultInstance.withAbsolute(absolute)
      }
    }

    implicit val partyFilterDecoder: Decoder[StreamConfig.PartyFilter] =
      (c: HCursor) => {
        for {
          party <- c.downField("party").as[String]
          templates <- c.downField("templates").as[Option[List[String]]]
          interfaces <- c.downField("interfaces").as[Option[List[String]]]
        } yield StreamConfig.PartyFilter(
          party,
          templates.getOrElse(List.empty),
          interfaces.getOrElse(List.empty),
        )
      }

    implicit val partySetTemplateFilterDecoder: Decoder[StreamConfig.PartyNamePrefixFilter] =
      (c: HCursor) => {
        for {
          partyNamePrefix <- c.downField("party_name_prefix").as[String]
          templates <- c.downField("templates").as[Option[List[String]]]
          interfaces <- c.downField("interfaces").as[Option[List[String]]]
        } yield StreamConfig.PartyNamePrefixFilter(
          partyNamePrefix,
          templates.getOrElse(List.empty),
          interfaces.getOrElse(List.empty),
        )
      }

    implicit val transactionStreamDecoder: Decoder[StreamConfig.TransactionsStreamConfig] =
      (c: HCursor) => {
        for {
          name <- c.downField("name").as[String]
          filters <- c.downField("filters").as[Option[List[PartyFilter]]]
          beginOffset <- c.downField("begin_offset").as[Option[LedgerOffset]]
          endOffset <- c.downField("end_offset").as[Option[LedgerOffset]]
          partyNamePrefixFilters <- c
            .downField("filter_by_party_set")
            .as[Option[List[PartyNamePrefixFilter]]]
          objectives <- c.downField("objectives").as[Option[StreamConfig.TransactionObjectives]]
          subscriptionDelay <- c
            .downField("subscription_delay")
            .as[Option[FiniteDuration]]
          maxItemCount <- c.downField("max_item_count").as[Option[Long]]
          timeout <- c
            .downField("timeout")
            .as[Option[FiniteDuration]]
        } yield StreamConfig.TransactionsStreamConfig(
          name = name,
          filters = filters.getOrElse(List.empty),
          partyNamePrefixFilters = partyNamePrefixFilters.getOrElse(List.empty),
          beginOffset = beginOffset,
          endOffset = endOffset,
          objectives = objectives,
          subscriptionDelay = subscriptionDelay,
          maxItemCount = maxItemCount,
          timeoutDurationO = timeout,
        )
      }

    implicit val transactionTreesStreamDecoder: Decoder[StreamConfig.TransactionTreesStreamConfig] =
      (c: HCursor) => {
        for {
          name <- c.downField("name").as[String]
          filters <- c.downField("filters").as[Option[List[PartyFilter]]]
          beginOffset <- c.downField("begin_offset").as[Option[LedgerOffset]]
          endOffset <- c.downField("end_offset").as[Option[LedgerOffset]]
          partyNamePrefixFilters <- c
            .downField("filter_by_party_set")
            .as[Option[List[PartyNamePrefixFilter]]]
          objectives <- c.downField("objectives").as[Option[StreamConfig.TransactionObjectives]]
          subscriptionDelay <- c
            .downField("subscription_delay")
            .as[Option[FiniteDuration]]
          maxItemCount <- c.downField("max_item_count").as[Option[Long]]
          timeout <- c
            .downField("timeout")
            .as[Option[FiniteDuration]]
        } yield StreamConfig.TransactionTreesStreamConfig(
          name = name,
          filters = filters.getOrElse(List.empty),
          partyNamePrefixFilters = partyNamePrefixFilters.getOrElse(List.empty),
          beginOffset = beginOffset,
          endOffset = endOffset,
          objectives = objectives,
          subscriptionDelay = subscriptionDelay,
          maxItemCount = maxItemCount,
          timeoutDurationO = timeout,
        )
      }

    implicit val activeContractsStreamDecoder: Decoder[StreamConfig.ActiveContractsStreamConfig] =
      (c: HCursor) => {
        for {
          name <- c.downField("name").as[String]
          filters <- c.downField("filters").as[Option[List[PartyFilter]]]
          partyNamePrefixFilters <- c
            .downField("filter_by_party_set")
            .as[Option[List[PartyNamePrefixFilter]]]
          objectives <- c
            .downField("objectives")
            .as[Option[StreamConfig.AcsAndCompletionsObjectives]]
          subscriptionDelay <- c
            .downField("subscription_delay")
            .as[Option[FiniteDuration]]
          maxItemCount <- c.downField("max_item_count").as[Option[Long]]
          timeout <- c
            .downField("timeout")
            .as[Option[FiniteDuration]]
        } yield StreamConfig.ActiveContractsStreamConfig(
          name = name,
          filters = filters.getOrElse(List.empty),
          partyNamePrefixFilters = partyNamePrefixFilters.getOrElse(List.empty),
          objectives = objectives,
          subscriptionDelay = subscriptionDelay,
          maxItemCount = maxItemCount,
          timeoutDurationO = timeout,
        )
      }
    Decoder.forProduct7(
      "name",
      "filters",
      "filter_by_party_set",
      "objectives",
      "subscription_delay",
      "max_item_count",
      "timeout",
    )(StreamConfig.ActiveContractsStreamConfig.apply)

    implicit val completionsStreamDecoder: Decoder[StreamConfig.CompletionsStreamConfig] =
      (c: HCursor) => {
        for {
          name <- c.downField("name").as[String]
          parties <- c.downField("parties").as[List[String]]
          applicationId <- c.downField("application_id").as[String]
          beginOffset <- c.downField("begin_offset").as[Option[LedgerOffset]]
          objectives <- c
            .downField("objectives")
            .as[Option[StreamConfig.AcsAndCompletionsObjectives]]
          subscriptionDelay <- c
            .downField("subscription_delay")
            .as[Option[FiniteDuration]]
          maxItemCount <- c.downField("max_item_count").as[Option[Long]]
          timeout <- c
            .downField("timeout")
            .as[Option[FiniteDuration]]
        } yield StreamConfig.CompletionsStreamConfig(
          name = name,
          parties = parties,
          applicationId = applicationId,
          beginOffset = beginOffset,
          objectives = objectives,
          subscriptionDelay = subscriptionDelay,
          maxItemCount = maxItemCount,
          timeoutDurationO = timeout,
        )
      }

    implicit val streamConfigDecoder: Decoder[StreamConfig] =
      Decoder
        .forProduct1[String, String]("type")(identity)
        .flatMap[StreamConfig] {
          case "transactions" => transactionStreamDecoder.widen
          case "transaction-trees" => transactionTreesStreamDecoder.widen
          case "active-contracts" => activeContractsStreamDecoder.widen
          case "completions" => completionsStreamDecoder.widen
          case invalid => Decoder.failedWithMessage(s"Invalid stream type: $invalid")
        }

    implicit val contractDescriptionDecoder: Decoder[FooSubmissionConfig.ContractDescription] =
      Decoder.forProduct3(
        "template",
        "weight",
        "payload_size_bytes",
      )(FooSubmissionConfig.ContractDescription.apply)

    implicit val nonconsumingExercisesDecoder: Decoder[FooSubmissionConfig.NonconsumingExercises] =
      Decoder.forProduct2(
        "probability",
        "payload_size_bytes",
      )(FooSubmissionConfig.NonconsumingExercises.apply)

    implicit val consumingExercisesDecoder: Decoder[FooSubmissionConfig.ConsumingExercises] =
      Decoder.forProduct2(
        "probability",
        "payload_size_bytes",
      )(FooSubmissionConfig.ConsumingExercises.apply)

    implicit val applicationIdConfigDecoder: Decoder[FooSubmissionConfig.ApplicationId] =
      Decoder.forProduct2(
        "id",
        "weight",
      )(FooSubmissionConfig.ApplicationId.apply)

    implicit val partySetDecoder: Decoder[FooSubmissionConfig.PartySet] =
      Decoder.forProduct3(
        "party_name_prefix",
        "count",
        "visibility",
      )(FooSubmissionConfig.PartySet.apply)

    implicit val fooSubmissionConfigDecoder: Decoder[FooSubmissionConfig] =
      (c: HCursor) => {
        for {
          numInstances <- c.downField("num_instances").as[Int]
          numObservers <- c.downField("num_observers").as[Int]
          uniqueObservers <- c.downField("unique_parties").as[Boolean]
          instancesDistribution <- c
            .downField("instance_distribution")
            .as[List[FooSubmissionConfig.ContractDescription]]
          numberOfDivulgees <- c.downField("num_divulgees").as[Option[Int]]
          numberOfExtraSubmitters <- c.downField("num_extra_submitters").as[Option[Int]]
          nonConsumingExercises <- c
            .downField("nonconsuming_exercises")
            .as[Option[NonconsumingExercises]]
          consumingExercises <- c.downField("consuming_exercises").as[Option[ConsumingExercises]]
          applicationIds <- c
            .downField("application_ids")
            .as[Option[List[FooSubmissionConfig.ApplicationId]]]
          maybeWaitForSubmission <- c.downField("wait_for_submission").as[Option[Boolean]]
          observerPartySets <- c
            .downField("observers_party_sets")
            .as[Option[List[FooSubmissionConfig.PartySet]]]
        } yield FooSubmissionConfig(
          numInstances,
          numObservers,
          uniqueObservers,
          instancesDistribution,
          numberOfDivulgees.getOrElse(0),
          numberOfExtraSubmitters.getOrElse(0),
          nonConsumingExercises,
          consumingExercises,
          applicationIds.getOrElse(List.empty),
          maybeWaitForSubmission,
          observerPartySets.getOrElse(List.empty),
        )
      }

    implicit val fibonacciSubmissionConfigDecoder: Decoder[FibonacciSubmissionConfig] =
      Decoder.forProduct4(
        "num_instances",
        "unique_parties",
        "value",
        "wait_for_submission",
      )(FibonacciSubmissionConfig.apply)

    implicit val submissionConfigDecoder: Decoder[SubmissionConfig] =
      Decoder
        .forProduct1[String, String]("type")(identity)
        .flatMap[SubmissionConfig] {
          case "foo" => fooSubmissionConfigDecoder.widen
          case "fibonacci" => fibonacciSubmissionConfigDecoder.widen
          case invalid => Decoder.failedWithMessage(s"Invalid submission type: $invalid")
        }

    implicit val workflowConfigDecoder: Decoder[WorkflowConfig] =
      (c: HCursor) =>
        for {
          submission <- c.downField("submission").as[Option[SubmissionConfig]]
          streams <- c
            .downField("streams")
            .as[Option[List[WorkflowConfig.StreamConfig]]]
            .map(_.getOrElse(Nil))
        } yield WorkflowConfig(submission, streams)
  }

}
