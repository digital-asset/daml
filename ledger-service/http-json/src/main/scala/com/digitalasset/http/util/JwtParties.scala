// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package util

import domain.{JwtPayload, JwtWritePayload}
import com.daml.nonempty.NonEmptyReturningOps._
import scalaz.{\/, \/-, -\/, NonEmptyList}
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._

private[http] object JwtParties {
  import EndpointsCompanion.{Error, Unauthorized}

  // security check for readAs; we delegate the remainder to
  // the participant's check that the JWT itself is valid
  def ensureReadAsAllowedByJwt(
      readAs: Option[NonEmptyList[domain.Party]],
      jwtPayload: JwtPayload,
  ): Error \/ Unit = {
    val disallowedParties: Set[domain.Party] =
      readAs.cata((_.toSet.filterNot(jwtPayload.parties)), Set.empty)
    if (disallowedParties.isEmpty) \/-(())
    else {
      val err =
        s"$EnsureReadAsDisallowedError: ${disallowedParties mkString ", "}"
      -\/(Unauthorized(err))
    }
  }

  private[util] val EnsureReadAsDisallowedError = "Queried parties not allowed by given JWT token"

  def resolveRefParties(
      meta: Option[domain.CommandMeta],
      jwtPayload: JwtWritePayload,
  ): domain.PartySet = {
    val actAs = meta.flatMap(_.actAs) getOrElse jwtPayload.submitter
    val readAs = meta.flatMap(_.readAs) getOrElse jwtPayload.readAs
    actAs.toSet1 ++ readAs
  }
}
