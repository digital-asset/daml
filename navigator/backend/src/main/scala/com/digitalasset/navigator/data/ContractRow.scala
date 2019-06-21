// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.data

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.json.ApiCodecCompressed
import com.digitalasset.navigator.json.ApiCodecCompressed.JsonImplicits._
import com.digitalasset.navigator.model._

import scala.util.{Failure, Try}
import scalaz.syntax.tag._
import spray.json._

final case class ContractRow(
    id: String,
    templateId: String,
    archiveTransactionId: Option[String],
    argument: String,
    agreementText: Option[String],
    signatories: Seq[String],
    observers: Seq[String]
) {

  def toContract(types: PackageRegistry): Try[Contract] = {
    (for {
      id <- Try(ApiTypes.ContractId(id))
      tid <- Try(parseOpaqueIdentifier(templateId).get)
      template <- Try(types.template(tid).get)
      recArgAny <- Try(
        ApiCodecCompressed.jsValueToApiType(argument.parseJson, tid, types.damlLfDefDataType _))
      recArg <- Try(recArgAny.asInstanceOf[ApiRecord])
    } yield {
      Contract(id, template, recArg, agreementText, signatories, observers)
    }).recoverWith {
      case e: Throwable =>
        Failure(DeserializationFailed(s"Failed to deserialize Contract from row: $this. Error: $e"))
    }
  }
}

object ContractRow {
  def fromContract(c: Contract): ContractRow = {
    ContractRow(
      c.id.unwrap,
      c.template.id.asOpaqueString,
      None,
      c.argument.toJson.compactPrint,
      c.agreementText,
      c.signatories,
      c.observers)
  }
}
