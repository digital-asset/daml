// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.transaction.ParticipantPermissionX

class PartyToParticipantComputations(override protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  /** Compute the new list of permissions from existing permissions and permissions
    * that need to be added and removed.
    *
    * If a participant in `adds` is already permissioned, the permissions are updated.
    */
  def computeNewPermissions(
      existingPermissions: Map[ParticipantId, ParticipantPermissionX],
      adds: List[(ParticipantId, ParticipantPermissionX)] = Nil,
      removes: List[ParticipantId] = Nil,
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
    } yield existingPermissions.removedAll(removes) ++ adds

  }
}
