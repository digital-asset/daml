// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.domain.admin.v0.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub
import com.digitalasset.canton.domain.admin.v0 as adminproto
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerPruningStatus
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SequencerAdminCommands {

  abstract class BaseSequencerAdministrationCommands[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = SequencerAdministrationServiceStub

    override def createService(channel: ManagedChannel): SequencerAdministrationServiceStub =
      adminproto.SequencerAdministrationServiceGrpc.stub(channel)
  }

  final case object GetPruningStatus
      extends BaseSequencerAdministrationCommands[
        Empty,
        adminproto.SequencerPruningStatus,
        SequencerPruningStatus,
      ] {
    override def createRequest(): Either[String, Empty] = Right(Empty())
    override def submitRequest(
        service: SequencerAdministrationServiceStub,
        request: Empty,
    ): Future[adminproto.SequencerPruningStatus] =
      service.pruningStatus(request)
    override def handleResponse(
        response: adminproto.SequencerPruningStatus
    ): Either[String, SequencerPruningStatus] =
      SequencerPruningStatus.fromProtoV0(response).leftMap(_.toString)
  }
}
