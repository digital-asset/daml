// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.data

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.json.ApiCodecCompressed
import com.digitalasset.navigator.json.ModelCodec.JsonImplicits._
import com.digitalasset.navigator.json.ApiCodecCompressed.JsonImplicits._
import com.digitalasset.navigator.json.DamlLfCodec.JsonImplicits._
import com.digitalasset.navigator.model._

import scala.util.{Failure, Try}
import scalaz.syntax.tag._
import spray.json._

final case class EventRow(
    id: String,
    transactionId: String,
    workflowId: String,
    parentId: Option[String],
    contractId: String,
    witnessParties: String,
    subclassType: String,
    templateId: Option[String],
    recordArgument: Option[String],
    contractCreateEventId: Option[String],
    choice: Option[String],
    argumentValue: Option[String],
    actingParties: Option[String],
    isConsuming: Option[Boolean],
    agreementText: Option[String],
    signatories: Seq[String],
    observers: Seq[String]) {

  def toEvent(types: PackageRegistry): Try[Event] = {
    subclassType match {
      case "ContractCreated" =>
        (for {
          wp <- Try(witnessParties.parseJson.convertTo[List[ApiTypes.Party]])
          tpStr <- Try(templateId.get)
          tp <- Try(parseOpaqueIdentifier(tpStr).get)
          recArgJson <- Try(recordArgument.get)
          recArgAny <- Try(
            ApiCodecCompressed
              .jsValueToApiType(recArgJson.parseJson, tp, types.damlLfDefDataType _))
          recArg <- Try(recArgAny.asInstanceOf[ApiRecord])
        } yield {
          ContractCreated(
            ApiTypes.EventId(id),
            parentId.map(ApiTypes.EventId(_)),
            ApiTypes.TransactionId(transactionId),
            wp,
            ApiTypes.WorkflowId(workflowId),
            ApiTypes.ContractId(contractId),
            tp,
            recArg,
            agreementText,
            signatories,
            observers
          )
        }).recoverWith {
          case e: Throwable =>
            Failure(
              DeserializationFailed(
                s"Failed to deserialize ContractCreated from row: $this. Error: $e"))
        }
      case "ChoiceExercised" =>
        (for {
          wp <- Try(witnessParties.parseJson.convertTo[List[ApiTypes.Party]])
          createId <- Try(contractCreateEventId.get)
          chc <- Try(choice.get)
          argJson <- Try(argumentValue.get)
          tp <- Try(templateId.get)
          tid <- Try(parseOpaqueIdentifier(tp).get)
          t <- Try(types.template(tid).get)
          choiceType <- Try(
            t.choices.find(c => ApiTypes.Choice.unwrap(c.name) == chc).get.parameter)
          arg <- Try(
            ApiCodecCompressed
              .jsValueToApiType(argJson.parseJson, choiceType, types.damlLfDefDataType _))
          apJson <- Try(actingParties.get)
          ap <- Try(apJson.parseJson.convertTo[List[ApiTypes.Party]])
          consuming <- Try(isConsuming.get)
        } yield {
          ChoiceExercised(
            ApiTypes.EventId(id),
            parentId.map(ApiTypes.EventId(_)),
            ApiTypes.TransactionId(transactionId),
            wp,
            ApiTypes.WorkflowId(workflowId),
            ApiTypes.ContractId(contractId),
            ApiTypes.EventId(createId),
            tid,
            ApiTypes.Choice(chc),
            arg,
            ap,
            consuming
          )
        }).recoverWith {
          case e: Throwable =>
            Failure(
              DeserializationFailed(
                s"Failed to deserialize ChoiceExercised from row: $this. Error: $e"))
        }
      case s => Failure(DeserializationFailed(s"unknown subclass type for Event: $s"))
    }
  }
}

object EventRow {

  def fromEvent(event: Event): EventRow = {
    event match {
      case c: ContractCreated =>
        EventRow(
          c.id.unwrap,
          c.transactionId.unwrap,
          c.workflowId.unwrap,
          c.parentId.map(_.unwrap),
          c.contractId.unwrap,
          c.witnessParties.toJson.compactPrint,
          "ContractCreated",
          Some(c.templateId.asOpaqueString),
          Some(c.argument.toJson.compactPrint),
          None,
          None,
          None,
          None,
          None,
          c.agreementText,
          c.signatories,
          c.observers
        )
      case e: ChoiceExercised =>
        EventRow(
          e.id.unwrap,
          e.transactionId.unwrap,
          e.workflowId.unwrap,
          e.parentId.map(_.unwrap),
          e.contractId.unwrap,
          e.witnessParties.toJson.compactPrint,
          "ChoiceExercised",
          Some(e.templateId.asOpaqueString),
          None,
          Some(e.contractCreateEvent.unwrap),
          Some(e.choice.unwrap),
          Some(e.argument.toJson.compactPrint),
          Some(e.actingParties.toJson.compactPrint),
          Some(e.consuming),
          None,
          Seq.empty,
          Seq.empty
        )
    }
  }
}
