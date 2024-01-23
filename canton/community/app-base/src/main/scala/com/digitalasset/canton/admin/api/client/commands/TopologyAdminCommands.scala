// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v0
import com.digitalasset.canton.topology.admin.v0.InitializationServiceGrpc.InitializationServiceStub
import com.digitalasset.canton.topology.admin.v0.TopologyAggregationServiceGrpc.TopologyAggregationServiceStub
import com.google.protobuf.timestamp.Timestamp
import io.grpc.ManagedChannel

import java.time.Instant
import scala.concurrent.Future

// TODO(#15161): Move commands to other file, e.g. related to VaultAdministration
object TopologyAdminCommands {

  object Aggregation {

    abstract class BaseCommand[Req, Res, Result] extends GrpcAdminCommand[Req, Res, Result] {
      override type Svc = TopologyAggregationServiceStub
      override def createService(channel: ManagedChannel): TopologyAggregationServiceStub =
        v0.TopologyAggregationServiceGrpc.stub(channel)
    }

    final case class ListParties(
        filterDomain: String,
        filterParty: String,
        filterParticipant: String,
        asOf: Option[Instant],
        limit: PositiveInt,
    ) extends BaseCommand[v0.ListPartiesRequest, v0.ListPartiesResponse, Seq[ListPartiesResult]] {

      override def createRequest(): Either[String, v0.ListPartiesRequest] =
        Right(
          v0.ListPartiesRequest(
            filterDomain = filterDomain,
            filterParty = filterParty,
            filterParticipant = filterParticipant,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit.value,
          )
        )

      override def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v0.ListPartiesRequest,
      ): Future[v0.ListPartiesResponse] =
        service.listParties(request)

      override def handleResponse(
          response: v0.ListPartiesResponse
      ): Either[String, Seq[ListPartiesResult]] =
        response.results.traverse(ListPartiesResult.fromProtoV0).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListKeyOwners(
        filterDomain: String,
        filterKeyOwnerType: Option[MemberCode],
        filterKeyOwnerUid: String,
        asOf: Option[Instant],
        limit: PositiveInt,
    ) extends BaseCommand[v0.ListKeyOwnersRequest, v0.ListKeyOwnersResponse, Seq[
          ListKeyOwnersResult
        ]] {

      override def createRequest(): Either[String, v0.ListKeyOwnersRequest] =
        Right(
          v0.ListKeyOwnersRequest(
            filterDomain = filterDomain,
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit.value,
          )
        )

      override def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v0.ListKeyOwnersRequest,
      ): Future[v0.ListKeyOwnersResponse] =
        service.listKeyOwners(request)

      override def handleResponse(
          response: v0.ListKeyOwnersResponse
      ): Either[String, Seq[ListKeyOwnersResult]] =
        response.results.traverse(ListKeyOwnersResult.fromProtoV0).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object Init {

    final case class InitId(identifier: String, fingerprint: String)
        extends GrpcAdminCommand[v0.InitIdRequest, v0.InitIdResponse, UniqueIdentifier] {
      override type Svc = InitializationServiceStub

      override def createService(channel: ManagedChannel): InitializationServiceStub =
        v0.InitializationServiceGrpc.stub(channel)

      override def createRequest(): Either[String, v0.InitIdRequest] =
        Right(v0.InitIdRequest(identifier, fingerprint, instance = ""))

      override def submitRequest(
          service: InitializationServiceStub,
          request: v0.InitIdRequest,
      ): Future[v0.InitIdResponse] =
        service.initId(request)

      override def handleResponse(response: v0.InitIdResponse): Either[String, UniqueIdentifier] =
        UniqueIdentifier.fromProtoPrimitive_(response.uniqueIdentifier)
    }
  }
}
