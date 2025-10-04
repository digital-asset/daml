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
    // and legacy requests for which we want to preserve the documentation
    val getUpdatesRequestName = LegacyDTOs.GetUpdatesRequest.getClass.getSimpleName.replace("$", "")
    val transactionFilterName = LegacyDTOs.TransactionFilter.getClass.getSimpleName.replace("$", "")
    val submitAndWaitForTransactionTreeResponseName =
      LegacyDTOs.SubmitAndWaitForTransactionTreeResponse.getClass.getSimpleName.replace("$", "")
    val transactionTreeName = LegacyDTOs.TransactionTree.getClass.getSimpleName.replace("$", "")
    val getTransactionByIdName =
      LegacyDTOs.GetTransactionByIdRequest.getClass.getSimpleName.replace("$", "")
    val getTransactionByOffsetName =
      LegacyDTOs.GetTransactionByOffsetRequest.getClass.getSimpleName.replace("$", "")
    val getTransactionResponseName =
      LegacyDTOs.GetTransactionResponse.getClass.getSimpleName.replace("$", "")
    val getTransactionTreeResponseName =
      LegacyDTOs.GetTransactionTreeResponse.getClass.getSimpleName.replace("$", "")
    val getActiveContractsRequestName =
      LegacyDTOs.GetActiveContractsRequest.getClass.getSimpleName.replace("$", "")
    val getUpdateTreesResponseName =
      LegacyDTOs.GetUpdateTreesResponse.getClass.getSimpleName.replace("$", "")
    val treeEventName =
      LegacyDTOs.TreeEvent.getClass.getSimpleName.replace("$", "")
    normalizeName(msgName) match {
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
      case `submitAndWaitForTransactionTreeResponseName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                "Provided for backwards compatibility, it will be removed in the Canton version 3.5.0."
              ),
              fieldComments = Map.empty,
            )
          )
        )
      case `transactionTreeName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                  |Complete view of an on-ledger transaction.""".stripMargin
              ),
              fieldComments = Map(
                "updateId" ->
                  """Assigned by the server. Useful for correlating logs.
                    |Must be a valid LedgerString (as described in ``value.proto``).
                    |Required""".stripMargin,
                "commandId" ->
                  """The ID of the command which resulted in this transaction. Missing for everyone except the submitting party.
                    |Must be a valid LedgerString (as described in ``value.proto``).
                    |Optional""".stripMargin,
                "workflowId" ->
                  """The workflow ID used in command submission. Only set if the ``workflow_id`` for the command was set.
                    |Must be a valid LedgerString (as described in ``value.proto``).
                    |Optional""".stripMargin,
                "effectiveAt" ->
                  """Ledger effective time.
                    |Required""".stripMargin,
                "offset" ->
                  """The absolute offset. The details of this field are described in ``community/ledger-api/README.md``.
                    |Required, it is a valid absolute offset (positive integer).""".stripMargin,
                "eventsById" ->
                  """Changes to the ledger that were caused by this transaction. Nodes of the transaction tree.
                    |Each key must be a valid node ID (non-negative integer).
                    |Required""".stripMargin,
                "synchronizerId" ->
                  """A valid synchronizer id.
                    |Identifies the synchronizer that synchronized the transaction.
                    |Required""".stripMargin,
                "traceContext" ->
                  """Optional; ledger API trace context
                    |
                    |The trace context transported in this message corresponds to the trace context supplied
                    |by the client application in a HTTP2 header of the original command submission.
                    |We typically use a header to transfer this type of information. Here we use message
                    |body, because it is used in gRPC streams which do not support per message headers.
                    |This field will be populated with the trace context contained in the original submission.
                    |If that was not provided, a unique ledger-api-server generated trace context will be used
                    |instead.""".stripMargin,
                "recordTime" ->
                  """The time at which the transaction was recorded. The record time refers to the synchronizer
                    |which synchronized the transaction.
                    |Required""".stripMargin,
              ),
            )
          )
        )
      case `getTransactionResponseName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                "Provided for backwards compatibility, it will be removed in the Canton version 3.5.0."
              ),
              fieldComments = Map(
                "transaction" -> "Required"
              ),
            )
          )
        )
      case `getTransactionTreeResponseName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                "Provided for backwards compatibility, it will be removed in the Canton version 3.5.0."
              ),
              fieldComments = Map(
                "transaction" -> "Required"
              ),
            )
          )
        )
      case `getTransactionByIdName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                "Provided for backwards compatibility, it will be removed in the Canton version 3.5.0."
              ),
              fieldComments = Map(
                "updateId" ->
                  """The ID of a particular transaction.
                    |Must be a valid LedgerString (as described in ``value.proto``).
                    |Required""".stripMargin,
                "requestingParties" ->
                  """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                    |The parties whose events the client expects to see.
                    |Events that are not visible for the parties in this collection will not be present in the response.
                    |Each element must be a valid PartyIdString (as described in ``value.proto``).
                    |Optional for backwards compatibility for GetTransactionById request: if defined transaction_format must be
                    |unset (falling back to defaults).""".stripMargin,
                "transactionFormat" ->
                  """Optional for GetTransactionById request for backwards compatibility: defaults to a transaction_format, where:
                    |
                    |- event_format.filters_by_party will have template-wildcard filters for all the requesting_parties
                    |- event_format.filters_for_any_party is unset
                    |- event_format.verbose = true
                    |- transaction_shape = TRANSACTION_SHAPE_ACS_DELTA""".stripMargin,
              ),
            )
          )
        )
      case `getTransactionByOffsetName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                "Provided for backwards compatibility, it will be removed in the Canton version 3.5.0."
              ),
              fieldComments = Map(
                "offset" ->
                  """The offset of the transaction being looked up.
                    |Must be a valid absolute offset (positive integer).
                    |Required""".stripMargin,
                "requestingParties" ->
                  """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                    |The parties whose events the client expects to see.
                    |Events that are not visible for the parties in this collection will not be present in the response.
                    |Each element must be a valid PartyIdString (as described in ``value.proto``).
                    |Optional for backwards compatibility for GetTransactionByOffset request: if defined transaction_format must be
                    |unset (falling back to defaults).""".stripMargin,
                "transactionFormat" ->
                  """Optional for GetTransactionByOffset request for backwards compatibility: defaults to a TransactionFormat, where:
                    |
                    |- event_format.filters_by_party will have template-wildcard filters for all the requesting_parties
                    |- event_format.filters_for_any_party is unset
                    |- event_format.verbose = true
                    |- transaction_shape = TRANSACTION_SHAPE_ACS_DELTA""".stripMargin,
              ),
            )
          )
        )
      case `getActiveContractsRequestName` =>
        original.map(info =>
          info.copy(message =
            info.message.copy(fieldComments =
              info.message.fieldComments
                + ("verbose" ->
                  """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                  |If enabled, values served over the API will contain more information than strictly necessary to interpret the data.
                  |In particular, setting the verbose flag to true triggers the ledger to include labels for record fields.
                  |Optional, if specified event_format must be unset.""".stripMargin)
                + ("filter" ->
                  """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                  |Templates to include in the served snapshot, per party.
                  |Optional, if specified event_format must be unset, if not specified event_format must be set.""".stripMargin)
            )
          )
        )
      case `getUpdateTreesResponseName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                "Provided for backwards compatibility, it will be removed in the Canton version 3.5.0."
              ),
              fieldComments = Map.empty,
            )
          )
        )
      case `treeEventName` =>
        Some(
          MessageInfo(
            FieldData(
              comments = Some(
                """Provided for backwards compatibility, it will be removed in the Canton version 3.5.0.
                  |Each tree event message type below contains a ``witness_parties`` field which
                  |indicates the subset of the requested parties that can see the event
                  |in question.
                  |
                  |Note that transaction trees might contain events with
                  |_no_ witness parties, which were included simply because they were
                  |children of events which have witnesses.""".stripMargin
              ),
              fieldComments = Map(
                "created" ->
                  """The event as it appeared in the context of its original daml transaction on this participant node.
                    |In particular, the offset, node_id pair of the daml transaction are preserved.
                    |Required""".stripMargin
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
