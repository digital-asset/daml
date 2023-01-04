// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.data

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.lf.value.json.ApiCodecCompressed
import ApiCodecCompressed.JsonImplicits._
import com.daml.navigator.json.ModelCodec.JsonImplicits._
import com.daml.navigator.json.DamlLfCodec.JsonImplicits._
import com.daml.navigator.model._

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
    choice: Option[String],
    argumentValue: Option[String],
    actingParties: Option[String],
    isConsuming: Option[Boolean],
    agreementText: Option[String],
    signatories: String,
    observers: String,
    key: Option[String],
) {

  def toEvent(types: PackageRegistry): Try[Event] = {
    subclassType match {
      case "ContractCreated" =>
        (for {
          wp <- Try(witnessParties.parseJson.convertTo[List[ApiTypes.Party]])
          sig <- Try(signatories.parseJson.convertTo[List[ApiTypes.Party]])
          obs <- Try(observers.parseJson.convertTo[List[ApiTypes.Party]])
          tpStr <- Try(templateId.get)
          tp <- Try(parseOpaqueIdentifier(tpStr).get)
          recArgJson <- Try(recordArgument.get)
          recArgAny <- Try(
            ApiCodecCompressed
              .jsValueToApiValue(recArgJson.parseJson, tp, types.damlLfDefDataType _)
          )
          recArg <- Try(recArgAny.asInstanceOf[ApiRecord])
          template <- types
            .template(tp)
            .fold[Try[Template]](
              Failure(new RuntimeException(s"No template in package registry with identifier $tp"))
            )(Try(_))
          key <- Try(
            key.map(
              _.parseJson.convertTo[ApiValue](
                ApiCodecCompressed.apiValueJsonReader(template.key.get, types.damlLfDefDataType _)
              )
            )
          )
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
            sig,
            obs,
            key,
          )
        }).recoverWith { case e: Throwable =>
          Failure(
            DeserializationFailed(
              s"Failed to deserialize ContractCreated from row: $this. Error: $e"
            )
          )
        }
      case "ChoiceExercised" =>
        (for {
          wp <- Try(witnessParties.parseJson.convertTo[List[ApiTypes.Party]])
          chc <- Try(choice.get)
          argJson <- Try(argumentValue.get)
          tp <- Try(templateId.get)
          tid <- Try(parseOpaqueIdentifier(tp).get)
          t <- Try(types.template(tid).get)
          choiceType <- Try(
            t.choices.find(c => ApiTypes.Choice.unwrap(c.name) == chc).get.parameter
          )
          arg <- Try(
            ApiCodecCompressed
              .jsValueToApiValue(argJson.parseJson, choiceType, types.damlLfDefDataType _)
          )
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
            tid,
            ApiTypes.Choice(chc),
            arg,
            ap,
            consuming,
          )
        }).recoverWith { case e: Throwable =>
          Failure(
            DeserializationFailed(
              s"Failed to deserialize ChoiceExercised from row: $this. Error: $e"
            )
          )
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
          c.agreementText,
          c.signatories.toJson.compactPrint,
          c.observers.toJson.compactPrint,
          c.key.map(_.toJson.compactPrint),
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
          Some(e.choice.unwrap),
          Some(e.argument.toJson.compactPrint),
          Some(e.actingParties.toJson.compactPrint),
          Some(e.consuming),
          None,
          "[]",
          "[]",
          None,
        )
    }
  }
}
