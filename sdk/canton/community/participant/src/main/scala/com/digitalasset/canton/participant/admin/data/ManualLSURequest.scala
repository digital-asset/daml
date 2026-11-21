// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.participant.v30.PerformSynchronizerUpgradeRequest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PhysicalSynchronizerId

// See invariants in `checkInvariants` below
final case class ManualLSURequest private (
    currentPSId: PhysicalSynchronizerId,
    successorPSId: PhysicalSynchronizerId,
    upgradeTime: CantonTimestamp,
    successorConfig: SynchronizerConnectionConfig,
    successorConnectionValidation: SequencerConnectionValidation,
)

object ManualLSURequest {
  import scala.math.Ordered.orderingToOrdered

  def fromProtoV30(
      request: PerformSynchronizerUpgradeRequest
  ): ParsingResult[ManualLSURequest] =
    for {
      currentPSId <-
        PhysicalSynchronizerId
          .fromProtoPrimitive(
            request.physicalSynchronizerId,
            "physical_synchronizer_id",
          )

      successor <- request.successor
        .toRight(ProtoDeserializationError.FieldNotSet("successor"))

      successorPSId <- PhysicalSynchronizerId
        .fromProtoPrimitive(
          successor.physicalSynchronizerId,
          "successor.physical_synchronizer_id",
        )

      upgradeTime <- ProtoConverter
        .parseRequired(
          CantonTimestamp.fromProtoTimestamp,
          "successor.announced_upgrade_time",
          successor.announcedUpgradeTime,
        )

      successorConfig <- ProtoConverter
        .parseRequired(
          SynchronizerConnectionConfig.fromProtoV30,
          "successor.config",
          successor.config,
        )

      validation <- SequencerConnectionValidation.fromProtoV30(
        successor.sequencerConnectionValidation
      )

      _ <- checkInvariants(
        currentPSId = currentPSId,
        successorPSId = successorPSId,
      ).leftMap(InvariantViolation(None, _))

    } yield ManualLSURequest(
      currentPSId = currentPSId,
      successorPSId = successorPSId,
      upgradeTime = upgradeTime,
      successorConfig = successorConfig,
      successorConnectionValidation = validation,
    )

  private def checkInvariants(
      currentPSId: PhysicalSynchronizerId,
      successorPSId: PhysicalSynchronizerId,
  ): Either[String, Unit] = for {
    _ <- Either.cond(
      currentPSId.logical == successorPSId.logical,
      (),
      s"Current and successor physical synchronizer ids must have same logical ids. Found: $currentPSId and $successorPSId",
    )

    _ <- Either.cond(
      currentPSId < successorPSId,
      (),
      s"Current physical synchronizer id must be smaller than the successor. Found: $currentPSId and $successorPSId",
    )
  } yield ()
}
