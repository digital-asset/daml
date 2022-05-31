// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import io.circe.{Decoder, HCursor}
import io.circe.yaml.parser
import cats.syntax.functor._

import java.io.Reader

object WorkflowConfigParser {
  import WorkflowConfig._
  import Decoders._

  def parse(reader: Reader): Either[ParserError, WorkflowConfig] =
    parser
      .parse(reader)
      .flatMap(_.as[WorkflowConfig])
      .left
      .map(error => ParserError(error.getLocalizedMessage))

  case class ParserError(details: String)

  object Decoders {
    implicit val transactionObjectivesDecoder: Decoder[StreamConfig.TransactionObjectives] =
      Decoder.forProduct4(
        "max_delay_seconds",
        "min_consumption_speed",
        "min_item_rate",
        "max_item_rate",
      )(StreamConfig.TransactionObjectives.apply)

    implicit val rateObjectivesDecoder: Decoder[StreamConfig.RateObjectives] =
      Decoder.forProduct2(
        "min_item_rate",
        "max_item_rate",
      )(StreamConfig.RateObjectives.apply)

    implicit val offsetDecoder: Decoder[LedgerOffset] = {
      Decoder.decodeString.map {
        case "ledger-begin" => LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
        case "ledger-end" => LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)
        case absolute => LedgerOffset.defaultInstance.withAbsolute(absolute)
      }
    }

    implicit val partyFilterDecoder: Decoder[StreamConfig.PartyFilter] =
      Decoder.forProduct2(
        "party",
        "templates",
      )(StreamConfig.PartyFilter.apply)

    implicit val transactionStreamDecoder: Decoder[StreamConfig.TransactionsStreamConfig] =
      Decoder.forProduct6(
        "name",
        "filters",
        "begin_offset",
        "end_offset",
        "objectives",
        "max_item_count",
      )(StreamConfig.TransactionsStreamConfig.apply)

    implicit val transactionTreesStreamDecoder: Decoder[StreamConfig.TransactionTreesStreamConfig] =
      Decoder.forProduct6(
        "name",
        "filters",
        "begin_offset",
        "end_offset",
        "objectives",
        "max_item_count",
      )(StreamConfig.TransactionTreesStreamConfig.apply)

    implicit val activeContractsStreamDecoder: Decoder[StreamConfig.ActiveContractsStreamConfig] =
      Decoder.forProduct4(
        "name",
        "filters",
        "objectives",
        "max_item_count",
      )(StreamConfig.ActiveContractsStreamConfig.apply)

    implicit val completionsStreamDecoder: Decoder[StreamConfig.CompletionsStreamConfig] =
      Decoder.forProduct7(
        "name",
        "parties",
        "application_id",
        "begin_offset",
        "timeout_in_seconds",
        "objectives",
        "max_item_count",
      )(StreamConfig.CompletionsStreamConfig.apply)

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

    implicit val fooSubmissionConfigDecoder: Decoder[FooSubmissionConfig] =
      Decoder.forProduct10(
        "num_instances",
        "num_observers",
        "num_divulgees",
        "num_extra_submitters",
        "unique_parties",
        "instance_distribution",
        "nonconsuming_exercises",
        "consuming_exercises",
        "application_ids",
        "wait_for_submission",
      )(FooSubmissionConfig.apply)

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
