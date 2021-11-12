// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
//package com.daml.ledger.api.benchtool
//
//import io.circe.Decoder
//import io.circe.yaml.parser
//
//import java.io.Reader
//
//object WorkflowParser {
//  import Decoders._
//
//  def parse(reader: Reader): Either[ParserError, WorkflowDescriptor] =
//    parser
//      .parse(reader)
//      .flatMap(_.as[WorkflowDescriptor])
//      .left
//      .map(error => ParserError(error.getLocalizedMessage))
//
//  case class ParserError(details: String)
//
//  object Decoders {
//    implicit val contractDescriptionDecoder: Decoder[SubmissionDescriptor.ContractDescription] =
//      Decoder.forProduct4(
//        "template",
//        "weight",
//        "payload_size_bytes",
//        "archive_probability",
//      )(SubmissionDescriptor.ContractDescription.apply)
//
//    implicit val submissionDescriptorDecoder: Decoder[SubmissionDescriptor] =
//      Decoder.forProduct4(
//        "num_instances",
//        "num_observers",
//        "unique_parties",
//        "instance_distribution",
//      )(SubmissionDescriptor.apply)
//
//    implicit val streamTypeDecoder: Decoder[StreamDescriptor.StreamType] = Decoder[String].emap {
//      case "active-contracts" => Right(StreamDescriptor.StreamType.ActiveContracts)
//      case "transactions" => Right(StreamDescriptor.StreamType.Transactions)
//      case "transaction-trees" => Right(StreamDescriptor.StreamType.TransactionTrees)
//      case invalid => Left(s"Invalid stream type: $invalid")
//    }
//
//    implicit val filtersDecoder: Decoder[StreamDescriptor.PartyFilter] =
//      Decoder.forProduct2(
//        "party",
//        "templates",
//      )(StreamDescriptor.PartyFilter.apply)
//
//    implicit val streamDescriptorDecoder: Decoder[StreamDescriptor] =
//      Decoder.forProduct3(
//        "type",
//        "name",
//        "filters",
//      )(StreamDescriptor.apply)
//
//    implicit val workflowDescriptorDecoder: Decoder[WorkflowDescriptor] =
//      Decoder.forProduct2(
//        "submission",
//        "streams",
//      )(WorkflowDescriptor.apply)
//  }
//
//}
