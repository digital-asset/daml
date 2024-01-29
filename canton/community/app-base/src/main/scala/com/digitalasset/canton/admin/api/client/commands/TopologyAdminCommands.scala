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
import com.digitalasset.canton.topology.admin.v30.TopologyAggregationServiceGrpc.TopologyAggregationServiceStub
import com.digitalasset.canton.topology.admin.v30old.InitializationServiceGrpc.InitializationServiceStub
import com.digitalasset.canton.topology.admin.{v30, v30old}
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
        v30.TopologyAggregationServiceGrpc.stub(channel)
    }

    final case class ListParties(
        filterDomain: String,
        filterParty: String,
        filterParticipant: String,
        asOf: Option[Instant],
        limit: PositiveInt,
    ) extends BaseCommand[v30.ListPartiesRequest, v30.ListPartiesResponse, Seq[
          ListPartiesResult
        ]] {

      override def createRequest(): Either[String, v30.ListPartiesRequest] =
        Right(
          v30.ListPartiesRequest(
            filterDomain = filterDomain,
            filterParty = filterParty,
            filterParticipant = filterParticipant,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit.value,
          )
        )

      override def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v30.ListPartiesRequest,
      ): Future[v30.ListPartiesResponse] =
        service.listParties(request)

      override def handleResponse(
          response: v30.ListPartiesResponse
      ): Either[String, Seq[ListPartiesResult]] =
        response.results.traverse(ListPartiesResult.fromProtoV30).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListKeyOwners(
        filterDomain: String,
        filterKeyOwnerType: Option[MemberCode],
        filterKeyOwnerUid: String,
        asOf: Option[Instant],
        limit: PositiveInt,
    ) extends BaseCommand[v30.ListKeyOwnersRequest, v30.ListKeyOwnersResponse, Seq[
          ListKeyOwnersResult
        ]] {

      override def createRequest(): Either[String, v30.ListKeyOwnersRequest] =
        Right(
          v30.ListKeyOwnersRequest(
            filterDomain = filterDomain,
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit.value,
          )
        )

      override def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v30.ListKeyOwnersRequest,
      ): Future[v30.ListKeyOwnersResponse] =
        service.listKeyOwners(request)

      override def handleResponse(
          response: v30.ListKeyOwnersResponse
      ): Either[String, Seq[ListKeyOwnersResult]] =
        response.results.traverse(ListKeyOwnersResult.fromProtoV30).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object Init {

    final case class InitId(identifier: String, fingerprint: String)
        extends GrpcAdminCommand[v30old.InitIdRequest, v30old.InitIdResponse, UniqueIdentifier] {
      override type Svc = InitializationServiceStub

      override def createService(channel: ManagedChannel): InitializationServiceStub =
        v30old.InitializationServiceGrpc.stub(channel)

      override def createRequest(): Either[String, v30old.InitIdRequest] =
        Right(v30old.InitIdRequest(identifier, fingerprint, instance = ""))

      override def submitRequest(
          service: InitializationServiceStub,
          request: v30old.InitIdRequest,
      ): Future[v30old.InitIdResponse] =
        service.initId(request)

      override def handleResponse(
          response: v30old.InitIdResponse
      ): Either[String, UniqueIdentifier] =
        UniqueIdentifier.fromProtoPrimitive_(response.uniqueIdentifier)
    }
  }
}
