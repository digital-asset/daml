// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v2.state_service
import com.daml.ledger.api.v2.transaction_filter
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{JsCantonError, JsEvent}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import com.digitalasset.canton.http.json.v2.JsContractEntry.{
  JsActiveContract,
  JsContractEntry,
  JsIncompleteAssigned,
  JsIncompleteUnassigned,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import sttp.tapir.query

import scala.concurrent.{ExecutionContext, Future}

class JsStateService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
) extends Endpoints
    with NamedLogging {

  import JsStateServiceCodecs.*

  private lazy val state = v2Endpoint.in(sttp.tapir.stringToPath("state"))
//  private lazy val wsState = wsEndpoint.in("state")

  private def stateServiceClient(token: Option[String] = None)(implicit
      traceContext: TraceContext
  ): state_service.StateServiceGrpc.StateServiceStub =
    ledgerClient.serviceClient(state_service.StateServiceGrpc.stub, token)

  def endpoints() = List(
    websocket(
      state.get
        .in(sttp.tapir.stringToPath("active-contracts"))
        .description("Get active contracts stream"),
      getActiveContractsStream,
    ),
    json(
      state.get
        .in(sttp.tapir.stringToPath("connected-domains"))
        .in(query[String]("party"))
        .description("Get connected domains"),
      getConnectedDomains,
    ),
    json(
      state.get
        .in(sttp.tapir.stringToPath("ledger-end"))
        .description("Get ledger end"),
      getLedgerEnd,
    ),
    json(
      state.get
        .in(sttp.tapir.stringToPath("latest-pruned-offsets"))
        .description("Get latest pruned offsets"),
      getLatestPrunedOffsets,
    ),
  )

  private def getConnectedDomains(
      callerContext: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, state_service.GetConnectedDomainsResponse]
  ] = req =>
    stateServiceClient(callerContext.token())(req.traceContext)
      .getConnectedDomains(state_service.GetConnectedDomainsRequest(party = req.in))
      .resultToRight

  private def getLedgerEnd(
      callerContext: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, state_service.GetLedgerEndResponse]
  ] = req =>
    stateServiceClient(callerContext.token())(req.traceContext)
      .getLedgerEnd(state_service.GetLedgerEndRequest())
      .resultToRight

  private def getLatestPrunedOffsets(
      callerContext: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, state_service.GetLatestPrunedOffsetsResponse]
  ] = req =>
    stateServiceClient(callerContext.token())(req.traceContext)
      .getLatestPrunedOffsets(state_service.GetLatestPrunedOffsetsRequest())
      .resultToRight

  private def getActiveContractsStream(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[
    state_service.GetActiveContractsRequest,
    JsGetActiveContractsResponse,
    NotUsed,
  ] =
    _ => {
      Flow[state_service.GetActiveContractsRequest]
        .flatMapConcat { req =>
          ClientAdapter
            .serverStreaming(
              req,
              stateServiceClient(caller.token())(TraceContext.empty).getActiveContracts,
            )
            .mapAsync(1)(r =>
              protocolConverters.GetActiveContractsResponse.toJson(r)(caller.token())
            )
        }
    }
}

object JsContractEntry {
  sealed trait JsContractEntry

  case object JsEmpty extends JsContractEntry
  case class JsIncompleteAssigned(assigned_event: JsAssignedEvent) extends JsContractEntry
  case class JsIncompleteUnassigned(
      created_event: JsEvent.CreatedEvent,
      unassigned_event: JsUnassignedEvent,
  ) extends JsContractEntry

  case class JsActiveContract(
      created_event: JsEvent.CreatedEvent,
      domain_id: String,
      reassignment_counter: Long,
  ) extends JsContractEntry
}

final case class JsAssignedEvent(
    source: String,
    target: String,
    unassign_id: String,
    submitter: String,
    reassignment_counter: Long,
    created_event: JsEvent.CreatedEvent,
)

final case class JsUnassignedEvent(
    unassign_id: String,
    contract_id: String,
    template_id: String,
    source: String,
    target: String,
    submitter: String,
    reassignment_counter: Long,
    assignment_exclusivity: Option[com.google.protobuf.timestamp.Timestamp],
    witness_parties: Seq[String],
    package_name: String,
)

final case class JsGetActiveContractsResponse(
    workflow_id: String,
    contract_entry: JsContractEntry,
)

object JsStateServiceCodecs {

  implicit val filtersRW: Codec[transaction_filter.Filters] = deriveCodec
  implicit val cumulativeFilterRW: Codec[transaction_filter.CumulativeFilter] = deriveCodec
  implicit val identifierFilterRW: Codec[transaction_filter.CumulativeFilter.IdentifierFilter] =
    deriveCodec

  implicit val wildcardFilterRW: Codec[transaction_filter.WildcardFilter] =
    deriveCodec
  implicit val templateFilterRW: Codec[transaction_filter.TemplateFilter] =
    deriveCodec

  implicit val interfaceFilterRW: Codec[transaction_filter.InterfaceFilter] =
    deriveCodec
  implicit val iwildcardFilterRW
      : Codec[transaction_filter.CumulativeFilter.IdentifierFilter.WildcardFilter] =
    deriveCodec

  implicit val itemplateFilterRW
      : Codec[transaction_filter.CumulativeFilter.IdentifierFilter.TemplateFilter] =
    deriveCodec
  implicit val iinterfaceFilterRW
      : Codec[transaction_filter.CumulativeFilter.IdentifierFilter.InterfaceFilter] =
    deriveCodec
  implicit val transactionFilterRW: Codec[transaction_filter.TransactionFilter] = deriveCodec
  implicit val getActiveContractsRequestRW: Codec[state_service.GetActiveContractsRequest] =
    deriveCodec

  implicit val jsGetActiveContractsResponseRW: Codec[JsGetActiveContractsResponse] = deriveCodec

  implicit val jsContractEntryRW: Codec[JsContractEntry] = deriveCodec
  implicit val jsIncompleteUnassignedRW: Codec[JsIncompleteUnassigned] = deriveCodec
  implicit val jsIncompleteAssignedRW: Codec[JsIncompleteAssigned] = deriveCodec
  implicit val jsActiveContractRW: Codec[JsActiveContract] = deriveCodec
  implicit val jsUnassignedEventRW: Codec[JsUnassignedEvent] = deriveCodec
  implicit val jsAssignedEventRW: Codec[JsAssignedEvent] = deriveCodec

  implicit val getConnectedDomainsRequestRW: Codec[state_service.GetConnectedDomainsRequest] =
    deriveCodec
  implicit val getConnectedDomainsResponseRW: Codec[state_service.GetConnectedDomainsResponse] =
    deriveCodec
  implicit val connectedDomainRW: Codec[state_service.GetConnectedDomainsResponse.ConnectedDomain] =
    deriveCodec
  implicit val participantPermissionRW: Codec[state_service.ParticipantPermission] = deriveCodec

  implicit val getLedgerEndRequestRW: Codec[state_service.GetLedgerEndRequest] = deriveCodec

  implicit val getLedgerEndResponseRW: Codec[state_service.GetLedgerEndResponse] = deriveCodec
  implicit val getLatestPrunedOffsetsResponseRW
      : Codec[state_service.GetLatestPrunedOffsetsResponse] =
    deriveCodec

}
