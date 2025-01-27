// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.admin.pruning.v30 as pruningProto
import com.digitalasset.canton.admin.sequencer.v30 as sequencerProto
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.v30 as proto
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.admin.grpc.InitializeSequencerResponse
import com.digitalasset.canton.synchronizer.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerTrafficStatus,
  TimestampSelector,
}
import com.digitalasset.canton.synchronizer.sequencer.{SequencerPruningStatus, SequencerSnapshot}
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
      proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub

    override def createService(
        channel: ManagedChannel
    ): proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub =
      proto.SequencerAdministrationServiceGrpc.stub(channel)
  }

  final case object GetPruningStatus
      extends BaseSequencerAdministrationCommand[
        proto.PruningStatusRequest,
        proto.PruningStatusResponse,
        SequencerPruningStatus,
      ] {
    override protected def createRequest(): Either[String, proto.PruningStatusRequest] = Right(
      proto.PruningStatusRequest()
    )
    override protected def submitRequest(
        service: proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: proto.PruningStatusRequest,
    ): Future[proto.PruningStatusResponse] =
      service.pruningStatus(request)
    override protected def handleResponse(
        response: proto.PruningStatusResponse
    ): Either[String, SequencerPruningStatus] =
      SequencerPruningStatus.fromProtoV30(response.getPruningStatus).leftMap(_.toString)
  }

  final case class GetTrafficControlState(
      members: Seq[Member],
      timestampSelector: TimestampSelector = TimestampSelector.LatestSafe,
  ) extends BaseSequencerAdministrationCommand[
        proto.TrafficControlStateRequest,
        proto.TrafficControlStateResponse,
        SequencerTrafficStatus,
      ] {
    override protected def createRequest(): Either[String, proto.TrafficControlStateRequest] =
      Right(
        proto
          .TrafficControlStateRequest(members.map(_.toProtoPrimitive), timestampSelector.toProtoV30)
      )
    override protected def submitRequest(
        service: proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: proto.TrafficControlStateRequest,
    ): Future[proto.TrafficControlStateResponse] =
      service.trafficControlState(request)
    override protected def handleResponse(
        response: proto.TrafficControlStateResponse
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
        proto.SetTrafficPurchasedRequest,
        proto.SetTrafficPurchasedResponse,
        Unit,
      ] {
    override protected def createRequest(): Either[String, proto.SetTrafficPurchasedRequest] =
      Right(
        proto.SetTrafficPurchasedRequest(member.toProtoPrimitive, serial.value, balance.value)
      )
    override protected def submitRequest(
        service: proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: proto.SetTrafficPurchasedRequest,
    ): Future[proto.SetTrafficPurchasedResponse] =
      service.setTrafficPurchased(request)
    override protected def handleResponse(
        response: proto.SetTrafficPurchasedResponse
    ): Either[String, Unit] = Either.unit
  }

  final case class InitializeFromOnboardingState(onboardingState: ByteString)
      extends GrpcAdminCommand[
        proto.InitializeSequencerFromOnboardingStateRequest,
        proto.InitializeSequencerFromOnboardingStateResponse,
        InitializeSequencerResponse,
      ] {
    override type Svc =
      proto.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub

    override def createService(
        channel: ManagedChannel
    ): proto.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      proto.SequencerInitializationServiceGrpc.stub(channel)

    override protected def submitRequest(
        service: proto.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
        request: proto.InitializeSequencerFromOnboardingStateRequest,
    ): Future[proto.InitializeSequencerFromOnboardingStateResponse] =
      GrpcStreamingUtils.streamToServer(
        service.initializeSequencerFromOnboardingState,
        (onboardingState: Array[Byte]) =>
          proto.InitializeSequencerFromOnboardingStateRequest(
            ByteString.copyFrom(onboardingState)
          ),
        request.onboardingState,
      )

    override protected def createRequest()
        : Either[String, proto.InitializeSequencerFromOnboardingStateRequest] =
      Right(
        proto.InitializeSequencerFromOnboardingStateRequest(onboardingState)
      )

    override protected def handleResponse(
        response: proto.InitializeSequencerFromOnboardingStateResponse
    ): Either[String, InitializeSequencerResponse] =
      Right(InitializeSequencerResponse(response.replicated))

    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class InitializeFromGenesisState(
      topologySnapshot: ByteString,
      synchronizerParameters: com.digitalasset.canton.protocol.StaticSynchronizerParameters,
  ) extends GrpcAdminCommand[
        proto.InitializeSequencerFromGenesisStateRequest,
        proto.InitializeSequencerFromGenesisStateResponse,
        InitializeSequencerResponse,
      ] {
    override type Svc =
      proto.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub

    override def createService(
        channel: ManagedChannel
    ): proto.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      proto.SequencerInitializationServiceGrpc.stub(channel)

    override protected def submitRequest(
        service: proto.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
        request: proto.InitializeSequencerFromGenesisStateRequest,
    ): Future[proto.InitializeSequencerFromGenesisStateResponse] =
      GrpcStreamingUtils.streamToServer(
        service.initializeSequencerFromGenesisState,
        (topologySnapshot: Array[Byte]) =>
          proto.InitializeSequencerFromGenesisStateRequest(
            topologySnapshot = ByteString.copyFrom(topologySnapshot),
            Some(synchronizerParameters.toProtoV30),
          ),
        request.topologySnapshot,
      )

    override protected def createRequest()
        : Either[String, proto.InitializeSequencerFromGenesisStateRequest] =
      Right(
        proto.InitializeSequencerFromGenesisStateRequest(
          topologySnapshot = topologySnapshot,
          Some(synchronizerParameters.toProtoV30),
        )
      )

    override protected def handleResponse(
        response: proto.InitializeSequencerFromGenesisStateResponse
    ): Either[String, InitializeSequencerResponse] =
      Right(InitializeSequencerResponse(response.replicated))

    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class Snapshot(timestamp: CantonTimestamp)
      extends BaseSequencerAdministrationCommand[
        proto.SnapshotRequest,
        proto.SnapshotResponse,
        SequencerSnapshot,
      ] {
    override protected def createRequest(): Either[String, proto.SnapshotRequest] =
      Right(proto.SnapshotRequest(Some(timestamp.toProtoTimestamp)))

    override protected def submitRequest(
        service: proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: proto.SnapshotRequest,
    ): Future[proto.SnapshotResponse] = service.snapshot(request)

    override protected def handleResponse(
        response: proto.SnapshotResponse
    ): Either[String, SequencerSnapshot] =
      response.value match {
        case proto.SnapshotResponse.Value
              .Failure(proto.SnapshotResponse.Failure(reason)) =>
          Left(reason)
        case proto.SnapshotResponse.Value
              .Success(proto.SnapshotResponse.Success(Some(result))) =>
          SequencerSnapshot.fromProtoV30(result).leftMap(_.toString)
        case proto.SnapshotResponse.Value
              .VersionedSuccess(proto.SnapshotResponse.VersionedSuccess(snapshot)) =>
          SequencerSnapshot
            .fromTrustedByteString(snapshot)
            .leftMap(_.toString)
        case _ => Left("response is empty")
      }

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class OnboardingState(
      observer: StreamObserver[proto.OnboardingStateResponse],
      sequencerOrTimestamp: Either[SequencerId, CantonTimestamp],
  ) extends BaseSequencerAdministrationCommand[
        proto.OnboardingStateRequest,
        CancellableContext,
        CancellableContext,
      ] {
    override protected def createRequest(): Either[String, proto.OnboardingStateRequest] =
      Right(
        proto.OnboardingStateRequest(request =
          sequencerOrTimestamp.fold[proto.OnboardingStateRequest.Request](
            sequencer =>
              proto.OnboardingStateRequest.Request
                .SequencerUid(sequencer.uid.toProtoPrimitive),
            timestamp => proto.OnboardingStateRequest.Request.Timestamp(timestamp.toProtoTimestamp),
          )
        )
      )

    override protected def submitRequest(
        service: proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: proto.OnboardingStateRequest,
    ): Future[CancellableContext] = {
      val context = Context.current().withCancellation()
      context.run(() => service.onboardingState(request, observer))
      Future.successful(context)
    }

    override protected def handleResponse(
        response: CancellableContext
    ): Either[String, CancellableContext] =
      Right(response)

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class DisableMember(member: Member)
      extends BaseSequencerAdministrationCommand[
        proto.DisableMemberRequest,
        proto.DisableMemberResponse,
        Unit,
      ] {
    override protected def createRequest(): Either[String, proto.DisableMemberRequest] =
      Right(proto.DisableMemberRequest(member.toProtoPrimitive))
    override protected def submitRequest(
        service: proto.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: proto.DisableMemberRequest,
    ): Future[proto.DisableMemberResponse] = service.disableMember(request)
    override protected def handleResponse(
        response: proto.DisableMemberResponse
    ): Either[String, Unit] = Either.unit
  }

  abstract class BaseSequencerPruningAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      proto.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): proto.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub =
      proto.SequencerPruningAdministrationServiceGrpc.stub(channel)
  }

  final case class Prune(timestamp: CantonTimestamp)
      extends BaseSequencerPruningAdministrationCommand[
        proto.PruneRequest,
        proto.PruneResponse,
        String,
      ] {
    override protected def createRequest(): Either[String, proto.PruneRequest] =
      Right(proto.PruneRequest(timestamp.toProtoTimestamp.some))

    override protected def submitRequest(
        service: proto.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub,
        request: proto.PruneRequest,
    ): Future[proto.PruneResponse] =
      service.prune(request)
    override protected def handleResponse(
        response: proto.PruneResponse
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
        pruningProto.LocatePruningTimestampRequest,
        pruningProto.LocatePruningTimestampResponse,
        Option[CantonTimestamp],
      ] {
    override protected def createRequest()
        : Either[String, pruningProto.LocatePruningTimestampRequest] =
      Right(
        pruningProto.LocatePruningTimestampRequest(index.value)
      )

    override protected def submitRequest(
        service: proto.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub,
        request: pruningProto.LocatePruningTimestampRequest,
    ): Future[pruningProto.LocatePruningTimestampResponse] =
      service.locatePruningTimestamp(request)

    override protected def handleResponse(
        response: pruningProto.LocatePruningTimestampResponse
    ): Either[String, Option[CantonTimestamp]] =
      response.timestamp.fold(Right(None): Either[String, Option[CantonTimestamp]])(
        CantonTimestamp.fromProtoTimestamp(_).bimap(_.message, Some(_))
      )
  }

  object Health {
    final case class SequencerStatusCommand()
        extends GrpcAdminCommand[
          sequencerProto.SequencerStatusRequest,
          sequencerProto.SequencerStatusResponse,
          NodeStatus[SequencerStatus],
        ] {

      override type Svc = sequencerProto.SequencerStatusServiceGrpc.SequencerStatusServiceStub

      override def createService(
          channel: ManagedChannel
      ): sequencerProto.SequencerStatusServiceGrpc.SequencerStatusServiceStub =
        sequencerProto.SequencerStatusServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: sequencerProto.SequencerStatusServiceGrpc.SequencerStatusServiceStub,
          request: sequencerProto.SequencerStatusRequest,
      ): Future[sequencerProto.SequencerStatusResponse] =
        service.sequencerStatus(request)

      override protected def createRequest()
          : Either[String, sequencerProto.SequencerStatusRequest] = Right(
        sequencerProto.SequencerStatusRequest()
      )

      override protected def handleResponse(
          response: sequencerProto.SequencerStatusResponse
      ): Either[String, NodeStatus[SequencerStatus]] =
        SequencerStatus.fromProtoV30(response).leftMap(_.message)
    }
  }
}
