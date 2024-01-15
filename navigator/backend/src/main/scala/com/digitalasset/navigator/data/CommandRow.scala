// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.data

import java.time.Instant

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.json.ApiCodecVerbose
import com.daml.lf.value.json.ApiCodecCompressed
import ApiCodecCompressed.JsonImplicits._
import com.daml.navigator.model._

import scala.util.{Failure, Try}
import scalaz.syntax.tag._
import spray.json._

final case class CommandRow(
    id: String,
    index: Long,
    workflowId: String,
    platformTime: String,
    subclassType: String,
    template: Option[String],
    recordArgument: Option[String],
    contractId: Option[String],
    interfaceId: Option[String],
    choice: Option[String],
    argumentValue: Option[String],
) {

  def toCommand(types: PackageRegistry): Try[Command] = {
    subclassType match {
      case "CreateCommand" =>
        (for {
          tp <- Try(template.get)
          tid <- Try(parseOpaqueIdentifier(tp).get)
          recArgJson <- Try(recordArgument.get)
          anyArg <- Try(
            ApiCodecCompressed
              .jsValueToApiValue(recArgJson.parseJson, tid, types.damlLfDefDataType _)
          )
          recArg <- Try(anyArg.asInstanceOf[ApiRecord])
        } yield {
          CreateCommand(
            ApiTypes.CommandId(id),
            index,
            ApiTypes.WorkflowId(workflowId),
            Instant.parse(platformTime),
            tid,
            recArg,
          )
        }).recoverWith { case e: Throwable =>
          Failure(
            DeserializationFailed(s"Failed to deserialize CreateCommand from row: $this. Error: $e")
          )
        }
      case "ExerciseCommand" =>
        (for {
          tp <- Try(template.get)
          tid <- Try(parseOpaqueIdentifier(tp).get)
          iidOp <- Try(interfaceId.map(parseOpaqueIdentifier(_).get))
          t <- Try(types.template(tid).get)
          cId <- Try(contractId.get)
          ch <- Try(choice.get)
          _ <- Try(t.choices.find(_.name.unwrap == ch).get)
          argJson <- Try(argumentValue.get)
          arg <- Try(ApiCodecVerbose.jsValueToApiValue(argJson.parseJson))
        } yield {
          ExerciseCommand(
            ApiTypes.CommandId(id),
            index,
            ApiTypes.WorkflowId(workflowId),
            Instant.parse(platformTime),
            ApiTypes.ContractId(cId),
            tid,
            iidOp,
            ApiTypes.Choice(ch),
            arg,
          )
        }).recoverWith { case e: Throwable =>
          Failure(
            DeserializationFailed(
              s"Failed to deserialize ExerciseCommand from row: $this. Error: $e"
            )
          )
        }
      case _ => Failure(DeserializationFailed(s"unknown subclass type for Command: $subclassType"))
    }
  }
}

object CommandRow {

  def fromCommand(c: Command): CommandRow = {
    c match {
      case c: CreateCommand =>
        CommandRow(
          c.id.unwrap,
          c.index,
          c.workflowId.unwrap,
          c.platformTime.toString,
          "CreateCommand",
          Some(c.template.asOpaqueString),
          Some(c.argument.toJson.compactPrint),
          None,
          None,
          None,
          None,
        )
      case e: ExerciseCommand =>
        CommandRow(
          c.id.unwrap,
          c.index,
          c.workflowId.unwrap,
          c.platformTime.toString,
          "ExerciseCommand",
          Some(e.template.asOpaqueString),
          None,
          Some(e.contract.unwrap),
          e.interfaceId.map(_.asOpaqueString),
          Some(e.choice.unwrap),
          Some(ApiCodecVerbose.apiValueToJsValue(e.argument).compactPrint),
        )
    }
  }
}
