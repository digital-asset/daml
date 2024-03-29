// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.pruning.v30.LocatePruningTimestamp
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.admin.grpc.InitializeSequencerResponse
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseSequencerAdminCommands {

  abstract class BaseSequencerAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub =
      v30.SequencerAdministrationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerPruningAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub =
      v30.SequencerPruningAdministrationServiceGrpc.stub(channel)
  }

  final case class InitializeFromOnboardingState(onboardingState: ByteString)
      extends GrpcAdminCommand[
        v30.InitializeSequencerFromOnboardingStateRequest,
        v30.InitializeSequencerFromOnboardingStateResponse,
        InitializeSequencerResponse,
      ] {
    override type Svc = v30.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub

    override def createService(
        channel: ManagedChannel
    ): v30.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      v30.SequencerInitializationServiceGrpc.stub(channel)

    override def submitRequest(
        service: v30.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
        request: v30.InitializeSequencerFromOnboardingStateRequest,
    ): Future[v30.InitializeSequencerFromOnboardingStateResponse] =
      service.initializeSequencerFromOnboardingState(request)

    override def createRequest()
        : Either[String, v30.InitializeSequencerFromOnboardingStateRequest] =
      Right(
        v30.InitializeSequencerFromOnboardingStateRequest(onboardingState)
      )

    override def handleResponse(
        response: v30.InitializeSequencerFromOnboardingStateResponse
    ): Either[String, InitializeSequencerResponse] =
      Right(InitializeSequencerResponse(response.replicated))
  }
  final case class InitializeFromGenesisState(
      topologySnapshot: ByteString,
      domainParameters: com.digitalasset.canton.protocol.StaticDomainParameters,
  ) extends GrpcAdminCommand[
        v30.InitializeSequencerFromGenesisStateRequest,
        v30.InitializeSequencerFromGenesisStateResponse,
        InitializeSequencerResponse,
      ] {
    override type Svc = v30.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub

    override def createService(
        channel: ManagedChannel
    ): v30.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      v30.SequencerInitializationServiceGrpc.stub(channel)

    override def submitRequest(
        service: v30.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
        request: v30.InitializeSequencerFromGenesisStateRequest,
    ): Future[v30.InitializeSequencerFromGenesisStateResponse] =
      service.initializeSequencerFromGenesisState(request)

    override def createRequest(): Either[String, v30.InitializeSequencerFromGenesisStateRequest] =
      Right(
        v30.InitializeSequencerFromGenesisStateRequest(
          topologySnapshot = topologySnapshot,
          Some(domainParameters.toProtoV30),
        )
      )

    override def handleResponse(
        response: v30.InitializeSequencerFromGenesisStateResponse
    ): Either[String, InitializeSequencerResponse] =
      Right(InitializeSequencerResponse(response.replicated))
  }

  final case class Snapshot(timestamp: CantonTimestamp)
      extends BaseSequencerAdministrationCommand[
        v30.SnapshotRequest,
        v30.SnapshotResponse,
        SequencerSnapshot,
      ] {
    override def createRequest(): Either[String, v30.SnapshotRequest] = {
      Right(v30.SnapshotRequest(Some(timestamp.toProtoTimestamp)))
    }

    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.SnapshotRequest,
    ): Future[v30.SnapshotResponse] = service.snapshot(request)

    override def handleResponse(
        response: v30.SnapshotResponse
    ): Either[String, SequencerSnapshot] =
      response.value match {
        case v30.SnapshotResponse.Value.Failure(v30.SnapshotResponse.Failure(reason)) =>
          Left(reason)
        case v30.SnapshotResponse.Value.Success(v30.SnapshotResponse.Success(Some(result))) =>
          SequencerSnapshot.fromProtoV30(result).leftMap(_.toString)
        case v30.SnapshotResponse.Value
              .VersionedSuccess(v30.SnapshotResponse.VersionedSuccess(snapshot)) =>
          SequencerSnapshot
            .fromTrustedByteString(snapshot)
            .leftMap(_.toString)
        case _ => Left("response is empty")
      }

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class OnboardingState(memberOrTimestamp: Either[SequencerId, CantonTimestamp])
      extends BaseSequencerAdministrationCommand[
        v30.OnboardingStateRequest,
        v30.OnboardingStateResponse,
        ByteString,
      ] {
    override def createRequest(): Either[String, v30.OnboardingStateRequest] = {
      Right(
        v30.OnboardingStateRequest(request =
          memberOrTimestamp.fold[v30.OnboardingStateRequest.Request](
            member => v30.OnboardingStateRequest.Request.SequencerId(member.toProtoPrimitive),
            timestamp => v30.OnboardingStateRequest.Request.Timestamp(timestamp.toProtoTimestamp),
          )
        )
      )
    }

    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.OnboardingStateRequest,
    ): Future[v30.OnboardingStateResponse] = service.onboardingState(request)

    override def handleResponse(
        response: v30.OnboardingStateResponse
    ): Either[String, ByteString] =
      response.value match {
        case v30.OnboardingStateResponse.Value
              .Failure(v30.OnboardingStateResponse.Failure(reason)) =>
          Left(reason)
        case v30.OnboardingStateResponse.Value
              .Success(
                v30.OnboardingStateResponse.Success(onboardingState)
              ) =>
          Right(onboardingState)
        case _ => Left("response is empty")
      }

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class GenesisState(
      timestamp: Option[CantonTimestamp]
  ) extends BaseSequencerAdministrationCommand[
        v30.GenesisStateRequest,
        v30.GenesisStateResponse,
        ByteString,
      ] {
    override def createRequest(): Either[String, v30.GenesisStateRequest] =
      Right(
        v30.GenesisStateRequest(
          timestamp = timestamp.map(_.toProtoTimestamp)
        )
      )

    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.GenesisStateRequest,
    ): Future[v30.GenesisStateResponse] = service.genesisState(request)

    override def handleResponse(
        response: v30.GenesisStateResponse
    ): Either[String, ByteString] =
      response.value match {
        case v30.GenesisStateResponse.Value
              .Failure(v30.GenesisStateResponse.Failure(reason)) =>
          Left(reason)
        case v30.GenesisStateResponse.Value
              .Success(
                v30.GenesisStateResponse.Success(genesisState)
              ) =>
          Right(genesisState)
        case _ => Left("response is empty")
      }

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class Prune(timestamp: CantonTimestamp)
      extends BaseSequencerPruningAdministrationCommand[
        v30.SequencerPruning.PruneRequest,
        v30.SequencerPruning.PruneResponse,
        String,
      ] {
    override def createRequest(): Either[String, v30.SequencerPruning.PruneRequest] =
      Right(v30.SequencerPruning.PruneRequest(timestamp.toProtoTimestamp.some))

    override def submitRequest(
        service: v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub,
        request: v30.SequencerPruning.PruneRequest,
    ): Future[v30.SequencerPruning.PruneResponse] =
      service.prune(request)
    override def handleResponse(
        response: v30.SequencerPruning.PruneResponse
    ): Either[String, String] =
      Either.cond(
        response.details.nonEmpty,
        response.details,
        "Pruning response did not contain details",
      )

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class LocatePruningTimestampCommand(index: PositiveInt)
      extends BaseSequencerPruningAdministrationCommand[
        LocatePruningTimestamp.Request,
        LocatePruningTimestamp.Response,
        Option[CantonTimestamp],
      ] {
    override def createRequest(): Either[String, LocatePruningTimestamp.Request] = Right(
      LocatePruningTimestamp.Request(index.value)
    )

    override def submitRequest(
        service: v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub,
        request: LocatePruningTimestamp.Request,
    ): Future[LocatePruningTimestamp.Response] =
      service.locatePruningTimestamp(request)

    override def handleResponse(
        response: LocatePruningTimestamp.Response
    ): Either[String, Option[CantonTimestamp]] =
      response.timestamp.fold(Right(None): Either[String, Option[CantonTimestamp]])(
        CantonTimestamp.fromProtoTimestamp(_).bimap(_.message, Some(_))
      )
  }

  final case class DisableMember(member: Member)
      extends BaseSequencerAdministrationCommand[
        v30.DisableMemberRequest,
        v30.DisableMemberResponse,
        Unit,
      ] {
    override def createRequest(): Either[String, v30.DisableMemberRequest] =
      Right(v30.DisableMemberRequest(member.toProtoPrimitive))
    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.DisableMemberRequest,
    ): Future[v30.DisableMemberResponse] = service.disableMember(request)
    override def handleResponse(response: v30.DisableMemberResponse): Either[String, Unit] = Right(
      ()
    )
  }
}
