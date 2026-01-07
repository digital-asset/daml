// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.admin.participant.v30 as proto
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.resource.{DbStorageMulti, Storage}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.{ExecutionContext, Future}

class GrpcParticipantReplicationService(
    storage: Storage,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends proto.ParticipantReplicationServiceGrpc.ParticipantReplicationService
    with NamedLogging {

  import ParticipantReplicationServiceError.*

  override def setPassive(request: proto.SetPassiveRequest): Future[proto.SetPassiveResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    storage match {
      case storageMulti: DbStorageMulti =>
        CantonGrpcUtil.mapErrNewEUS {
          storageMulti
            .setPassive()
            .leftMap(err => InternalError.Failure(err))
            .map(_ => proto.SetPassiveResponse())
        }

      case unsupportedStorage: Storage =>
        Future.failed(
          UnsupportedConfig.Failure(unsupportedStorage.getClass.getSimpleName).asGrpcError
        )
    }
  }
}

sealed trait ParticipantReplicationServiceError extends ContextualizedCantonError

object ParticipantReplicationServiceError extends ParticipantErrorGroup.ReplicationErrorGroup {

  @Explanation("Internal error emitted upon internal participant replication errors")
  @Resolution("Contact support")
  object InternalError
      extends ErrorCode(
        "PARTICIPANT_REPLICATION_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(error: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(error)
        with ParticipantReplicationServiceError
  }

  @Explanation(
    "Error emitted if the supplied storage configuration does not support replication."
  )
  @Resolution("Use a participant storage backend supporting replication.")
  object UnsupportedConfig
      extends ErrorCode(
        "PARTICIPANT_REPLICATION_NOT_SUPPORTED_BY_STORAGE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(unsupportedStorageName: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"Participant with storage $unsupportedStorageName does not support set passive."
        )
        with ParticipantReplicationServiceError
  }

}
