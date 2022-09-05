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
      Decoder.forProduct8(
        "name",
        "filters",
        "filter_by_party_set",
        "begin_offset",
        "end_offset",
        "objectives",
        "max_item_count",
        "timeout_in_seconds",
      )(StreamConfig.TransactionsStreamConfig.apply)

    implicit val transactionTreesStreamDecoder: Decoder[StreamConfig.TransactionTreesStreamConfig] =
      Decoder.forProduct8(
        "name",
        "filters",
        "filter_by_party_set",
        "begin_offset",
        "end_offset",
        "objectives",
        "max_item_count",
        "timeout_in_seconds",
      )(StreamConfig.TransactionTreesStreamConfig.apply)

    implicit val activeContractsStreamDecoder: Decoder[StreamConfig.ActiveContractsStreamConfig] =
      Decoder.forProduct6(
        "name",
        "filters",
        "filter_by_party_set",
        "objectives",
        "max_item_count",
        "timeout_in_seconds",
      )(StreamConfig.ActiveContractsStreamConfig.apply)

    implicit val completionsStreamDecoder: Decoder[StreamConfig.CompletionsStreamConfig] =
      Decoder.forProduct7(
        "name",
        "parties",
        "application_id",
        "begin_offset",
        "objectives",
        "max_item_count",
        "timeout_in_seconds",
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

    implicit val partySetDecoder: Decoder[FooSubmissionConfig.PartySet] =
      Decoder.forProduct3(
        "party_name_prefix",
        "count",
        "visibility",
      )(FooSubmissionConfig.PartySet.apply)

    implicit val fooSubmissionConfigDecoder: Decoder[FooSubmissionConfig] =
      Decoder.forProduct11(
        "num_instances",
        "num_observers",
        "unique_parties",
        "instance_distribution",
        "num_divulgees",
        "num_extra_submitters",
        "nonconsuming_exercises",
        "consuming_exercises",
        "application_ids",
        "wait_for_submission",
        "observers_party_set",
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
