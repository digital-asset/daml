// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.transaction.ParticipantPermissionX
import com.digitalasset.canton.tracing.TraceContext

class PartyToParticipantComputations(override protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  /** Compute the new list of permissions from existing permissions and permissions
    * that need to be added and removed.
    *
    * If a participant in `adds` is already permissioned, the previous permissions are kept.
    */
  def computeNewPermissions(
      existingPermissions: Map[ParticipantId, ParticipantPermissionX],
      adds: List[(ParticipantId, ParticipantPermissionX)] = Nil,
      removes: List[ParticipantId] = Nil,
  )(implicit
      traceContext: TraceContext
  ): Either[String, Map[ParticipantId, ParticipantPermissionX]] = {

    val conflictsO =
      NonEmpty.from(adds.map { case (participantId, _) => participantId }.intersect(removes))

    val conflictsCheck: Either[String, Unit] = conflictsO
      .toLeft(())
      .leftMap(conflicts =>
        s"Permissions for the following participant were found in adds and removes: $conflicts"
      )

    val unknownRemovesCheck = NonEmpty
      .from(removes.toSet.diff(existingPermissions.keySet))
      .toLeft(())
      .leftMap(unknowns =>
        s"Cannot remove permission for participants that are not permissioned: $unknowns"
      )

    for {
      _ <- conflictsCheck
      _ <- unknownRemovesCheck
    } yield adds.foldLeft(existingPermissions.removedAll(removes)) {
      case (updatedPermissions, (participantId, requestedPermissions)) =>
        updatedPermissions.get(participantId) match {
          case Some(existingPermissions) if existingPermissions != requestedPermissions =>
            logger.debug(
              s"Ignoring new permissions $requestedPermissions for participant $participantId and keeping $existingPermissions"
            )

            updatedPermissions

          case _ => updatedPermissions + (participantId -> requestedPermissions)
        }
    }
  }
}
