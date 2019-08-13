// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.data

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import ApiCodecCompressed.JsonImplicits._
import com.digitalasset.navigator.json.ModelCodec.JsonImplicits._
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
    signatories: String,
    observers: String,
    key: Option[String]
) {

  def toContract(types: PackageRegistry): Try[Contract] = {
    (for {
      id <- Try(ApiTypes.ContractId(id))
      tid <- Try(parseOpaqueIdentifier(templateId).get)
      template <- Try(types.template(tid).get)
      recArgAny <- Try(
        ApiCodecCompressed.jsValueToApiValue(argument.parseJson, tid, types.damlLfDefDataType _))
      recArg <- Try(recArgAny.asInstanceOf[ApiRecord])
      sig <- Try(signatories.parseJson.convertTo[List[ApiTypes.Party]])
      obs <- Try(signatories.parseJson.convertTo[List[ApiTypes.Party]])
      key <- Try(
        key.map(_.parseJson.convertTo[ApiValue](
          ApiCodecCompressed.apiValueJsonReader(template.key.get, types.damlLfDefDataType _))))
    } yield {
      Contract(id, template, recArg, agreementText, sig, obs, key)
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
      c.signatories.toJson.compactPrint,
      c.observers.toJson.compactPrint,
      c.key.map(_.toJson.compactPrint)
    )
  }
}
