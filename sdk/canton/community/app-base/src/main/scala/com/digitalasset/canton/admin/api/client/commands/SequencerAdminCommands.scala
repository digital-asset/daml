// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.{NodeStatus, SequencerStatus}
import com.digitalasset.canton.admin.domain.v30.SequencerStatusServiceGrpc.SequencerStatusServiceStub
import com.digitalasset.canton.admin.domain.v30.{
  SequencerStatusRequest,
  SequencerStatusResponse,
  SequencerStatusServiceGrpc,
}
import com.digitalasset.canton.admin.pruning.v30.LocatePruningTimestamp
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.admin.grpc.InitializeSequencerResponse
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerTrafficStatus,
  TimestampSelector,
}
import com.digitalasset.canton.domain.sequencing.sequencer.{
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.util.GrpcStreamingUtils
import com.google.protobuf.ByteString
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import scala.concurrent.Future

object SequencerAdminCommands {

  abstract class BaseSequencerAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub

    override def createService(
        channel: ManagedChannel
    ): v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub =
      v30.SequencerAdministrationServiceGrpc.stub(channel)
  }

  final case object GetPruningStatus
      extends BaseSequencerAdministrationCommand[
        v30.PruningStatusRequest,
        v30.PruningStatusResponse,
        SequencerPruningStatus,
      ] {
    override def createRequest(): Either[String, v30.PruningStatusRequest] = Right(
      v30.PruningStatusRequest()
    )
    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.PruningStatusRequest,
    ): Future[v30.PruningStatusResponse] =
      service.pruningStatus(request)
    override def handleResponse(
        response: v30.PruningStatusResponse
    ): Either[String, SequencerPruningStatus] =
      SequencerPruningStatus.fromProtoV30(response.getPruningStatus).leftMap(_.toString)
  }

  final case class GetTrafficControlState(
      members: Seq[Member],
      timestampSelector: TimestampSelector = TimestampSelector.LatestSafe,
  ) extends BaseSequencerAdministrationCommand[
        v30.TrafficControlStateRequest,
        v30.TrafficControlStateResponse,
        SequencerTrafficStatus,
      ] {
    override def createRequest(): Either[String, v30.TrafficControlStateRequest] = Right(
      v30
        .TrafficControlStateRequest(members.map(_.toProtoPrimitive), timestampSelector.toProtoV30)
    )
    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.TrafficControlStateRequest,
    ): Future[v30.TrafficControlStateResponse] =
      service.trafficControlState(request)
    override def handleResponse(
        response: v30.TrafficControlStateResponse
    ): Either[String, SequencerTrafficStatus] =
      response.trafficStates.toList
        .traverse { case (member, state) =>
          (
            Member.fromProtoPrimitive(member, "member").leftMap(_.toString),
            TrafficState.fromProtoV30(state).leftMap(_.toString).map(Right(_)),
          ).tupled
        }
        .map(_.toMap)
        .map(SequencerTrafficStatus.apply)
  }

  final case class SetTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      balance: NonNegativeLong,
  ) extends BaseSequencerAdministrationCommand[
        v30.SetTrafficPurchasedRequest,
        v30.SetTrafficPurchasedResponse,
        Unit,
      ] {
    override def createRequest(): Either[String, v30.SetTrafficPurchasedRequest] = Right(
      v30.SetTrafficPurchasedRequest(member.toProtoPrimitive, serial.value, balance.value)
    )
    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.SetTrafficPurchasedRequest,
    ): Future[v30.SetTrafficPurchasedResponse] =
      service.setTrafficPurchased(request)
    override def handleResponse(
        response: v30.SetTrafficPurchasedResponse
    ): Either[String, Unit] = Right(())
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
      GrpcStreamingUtils.streamToServer(
        service.initializeSequencerFromOnboardingState,
        (onboardingState: Array[Byte]) =>
          v30.InitializeSequencerFromOnboardingStateRequest(ByteString.copyFrom(onboardingState)),
        request.onboardingState,
      )

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
      GrpcStreamingUtils.streamToServer(
        service.initializeSequencerFromGenesisState,
        (topologySnapshot: Array[Byte]) =>
          v30.InitializeSequencerFromGenesisStateRequest(
            topologySnapshot = ByteString.copyFrom(topologySnapshot),
            Some(domainParameters.toProtoV30),
          ),
        request.topologySnapshot,
      )

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
    override def createRequest(): Either[String, v30.SnapshotRequest] =
      Right(v30.SnapshotRequest(Some(timestamp.toProtoTimestamp)))

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

  final case class OnboardingState(
      observer: StreamObserver[v30.OnboardingStateResponse],
      sequencerOrTimestamp: Either[SequencerId, CantonTimestamp],
  ) extends BaseSequencerAdministrationCommand[
        v30.OnboardingStateRequest,
        CancellableContext,
        CancellableContext,
      ] {
    override def createRequest(): Either[String, v30.OnboardingStateRequest] =
      Right(
        v30.OnboardingStateRequest(request =
          sequencerOrTimestamp.fold[v30.OnboardingStateRequest.Request](
            sequencer =>
              v30.OnboardingStateRequest.Request.SequencerUid(sequencer.uid.toProtoPrimitive),
            timestamp => v30.OnboardingStateRequest.Request.Timestamp(timestamp.toProtoTimestamp),
          )
        )
      )

    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.OnboardingStateRequest,
    ): Future[CancellableContext] = {
      val context = Context.current().withCancellation()
      context.run(() => service.onboardingState(request, observer))
      Future.successful(context)
    }

    override def handleResponse(response: CancellableContext): Either[String, CancellableContext] =
      Right(response)

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
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

  abstract class BaseSequencerPruningAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub =
      v30.SequencerPruningAdministrationServiceGrpc.stub(channel)
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

  object Health {
    final case class SequencerStatusCommand()
        extends GrpcAdminCommand[
          SequencerStatusRequest,
          SequencerStatusResponse,
          NodeStatus[SequencerStatus],
        ] {

      override type Svc = SequencerStatusServiceStub

      override def createService(channel: ManagedChannel): SequencerStatusServiceStub =
        SequencerStatusServiceGrpc.stub(channel)

      override def submitRequest(
          service: SequencerStatusServiceStub,
          request: SequencerStatusRequest,
      ): Future[SequencerStatusResponse] =
        service.sequencerStatus(request)

      override def createRequest(): Either[String, SequencerStatusRequest] = Right(
        SequencerStatusRequest()
      )

      override def handleResponse(
          response: SequencerStatusResponse
      ): Either[String, NodeStatus[SequencerStatus]] =
        SequencerStatus.fromProtoV30(response).leftMap(_.message)
    }
  }
}
