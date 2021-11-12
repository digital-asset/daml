// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import io.circe.Decoder
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
    implicit val objectivesDecoder: Decoder[StreamConfig.Objectives] =
      Decoder.forProduct2(
        "max_delay_seconds",
        "min_consumption_speed",
      )(StreamConfig.Objectives.apply)

    implicit val offsetDecoder: Decoder[LedgerOffset] =
      Decoder.decodeString.map(LedgerOffset.defaultInstance.withAbsolute)

    implicit val partyFilterDecoder: Decoder[StreamConfig.PartyFilter] =
      Decoder.forProduct2(
        "party",
        "templates",
      )(StreamConfig.PartyFilter.apply)

    implicit val transactionStreamDecoder: Decoder[StreamConfig.TransactionsStreamConfig] =
      Decoder.forProduct5(
        "name",
        "filters",
        "begin_offset",
        "end_offset",
        "objectives",
      )(StreamConfig.TransactionsStreamConfig.apply)

    implicit val transactionTreesStreamDecoder: Decoder[StreamConfig.TransactionTreesStreamConfig] =
      Decoder.forProduct5(
        "name",
        "filters",
        "begin_offset",
        "end_offset",
        "objectives",
      )(StreamConfig.TransactionTreesStreamConfig.apply)

    implicit val activeContractsStreamDecoder: Decoder[StreamConfig.ActiveContractsStreamConfig] =
      Decoder.forProduct2(
        "name",
        "filters",
      )(StreamConfig.ActiveContractsStreamConfig.apply)

    implicit val completionsStreamDecoder: Decoder[StreamConfig.CompletionsStreamConfig] =
      Decoder.forProduct4(
        "name",
        "party",
        "application_id",
        "begin_offset",
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

    implicit val contractDescriptionDecoder: Decoder[SubmissionConfig.ContractDescription] =
      Decoder.forProduct4(
        "template",
        "weight",
        "payload_size_bytes",
        "archive_probability",
      )(SubmissionConfig.ContractDescription.apply)

    implicit val submissionConfigDecoder: Decoder[SubmissionConfig] =
      Decoder.forProduct4(
        "num_instances",
        "num_observers",
        "unique_parties",
        "instance_distribution",
      )(SubmissionConfig.apply)

    implicit val workflowConfigDecoder: Decoder[WorkflowConfig] =
      Decoder.forProduct2(
        "submission",
        "streams",
      )(WorkflowConfig.apply)
  }

}
