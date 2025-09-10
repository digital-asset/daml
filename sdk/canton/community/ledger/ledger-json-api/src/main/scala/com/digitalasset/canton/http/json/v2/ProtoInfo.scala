// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.v2.ProtoInfo.{camelToSnake, normalizeName}
import io.circe
import io.circe.yaml.Printer

import scala.collection.immutable.SortedMap
import scala.io.Source

/** Reads stored proto data in order to extract comments (and put into openapi).
  *
  * The algorithms used are inefficient, but since the code is used only to generate documentation
  * it should not cause any problems.
  */

final case class ProtoInfo(protoComments: ExtractedProtoComments) {

  def findMessageInfo(
      msgName: String,
      parentMsgName: Option[String] = None,
  ): Option[MessageInfo] = {
    val original = protoComments.messages
      .get(msgName)
      .orElse(protoComments.messages.get(normalizeName(msgName)))
      .orElse(
        protoComments.oneOfs
          .get(normalizeName(parentMsgName.getOrElse("")))
          .flatMap(_.get(camelToSnake(normalizeName(msgName))))
      )
    // Special case for GetUpdatesRequest, where we want to document the verbose and filter fields
    // TODO(#27734) remove this hack when json legacy is removed
    val getUpdatesRequestName = LegacyDTOs.GetUpdatesRequest.getClass.getSimpleName.replace("$", "")
    val transactionFilterName = LegacyDTOs.TransactionFilter.getClass.getSimpleName.replace("$", "")
    msgName match {
      case `getUpdatesRequestName` =>
        original.map(info =>
          info.copy(message =
            info.message.copy(fieldComments =
              info.message.fieldComments
                + ("verbose" ->
                  """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                    |If enabled, values served over the API will contain more information than strictly necessary to interpret the data.
                    |In particular, setting the verbose flag to true triggers the ledger to include labels, record and variant type ids
                    |for record fields.
                    |Optional for backwards compatibility, if defined update_format must be unset""".stripMargin)
                + ("filter" ->
                  """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                    |Requesting parties with template filters.
                    |Template filters must be empty for GetUpdateTrees requests.
                    |Optional for backwards compatibility, if defined update_format must be unset""".stripMargin)
            )
          )
        )
      case `transactionFilterName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
              |Used both for filtering create and archive events as well as for filtering transaction trees.""".stripMargin
              ),
              fieldComments = Map(
                "filtersByParty" ->
                  """Each key must be a valid PartyIdString (as described in ``value.proto``).
                    |The interpretation of the filter depends on the transaction-shape being filtered:
                    |
                    |1. For **transaction trees** (used in GetUpdateTreesResponse for backwards compatibility) all party keys used as
                    |   wildcard filters, and all subtrees whose root has one of the listed parties as an informee are returned.
                    |   If there are ``CumulativeFilter``s, those will control returned ``CreatedEvent`` fields where applicable, but will
                    |   not be used for template/interface filtering.
                    |2. For **ledger-effects** create and exercise events are returned, for which the witnesses include at least one of
                    |   the listed parties and match the per-party filter.
                    |3. For **transaction and active-contract-set streams** create and archive events are returned for all contracts whose
                    |   stakeholders include at least one of the listed parties and match the per-party filter.""".stripMargin,
                "filtersForAnyParty" ->
                  """Wildcard filters that apply to all the parties existing on the participant. The interpretation of the filters is the same
                    |with the per-party filter as described above.""".stripMargin,
              ),
            )
          )
        )
      case _ => original
    }
  }
}

final case class MessageInfo(message: FieldData) {
  def getComments(): Option[String] = message.comments

  def getFieldComment(name: String): Option[String] =
    message.fieldComments
      .get(name)
      .orElse(message.fieldComments.get(camelToSnake(name)))

  override def toString: String = getComments().getOrElse("")
}

final case class FieldData(comments: Option[String], fieldComments: Map[String, String])

final case class ExtractedProtoComments(
    messages: SortedMap[String, MessageInfo],
    oneOfs: SortedMap[String, SortedMap[String, MessageInfo]],
) {
  def toYaml(): String = {
    import io.circe.syntax.*
    import io.circe.generic.auto.*
    val yamlPrinter = Printer(preserveOrder = true)
    yamlPrinter.pretty(this.asJson)
  }
}

object ProtoInfo {
  val LedgerApiDescriptionResourceLocation = "ledger-api/proto-data.yml"
  def camelToSnake(name: String): String =
    name
      .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .toLowerCase

  /** We drop initial `Js` prefix and single digits suffixes.
    */
  def normalizeName(s: String): String =
    if (s.nonEmpty && s.last.isDigit) s.dropRight(1) else if (s.startsWith("Js")) s.drop(2) else s

  def loadData(): Either[circe.Error, ProtoInfo] = {
    val res = Source.fromResource(
      LedgerApiDescriptionResourceLocation,
      classOf[com.digitalasset.canton.http.json.v2.ProtoInfo.type].getClassLoader,
    )
    val yaml = res.getLines().mkString("\n")
    import io.circe.generic.auto.*
    import io.circe.yaml.parser
    parser
      .parse(yaml)
      .flatMap(_.as[ExtractedProtoComments])
      .map(ProtoInfo(_))
  }
}
