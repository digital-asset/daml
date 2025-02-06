// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.http
import com.digitalasset.canton.http.{JwtPayload, JwtWritePayload}
import scalaz.syntax.foldable.*
import scalaz.syntax.std.option.*
import scalaz.{-\/, NonEmptyList, \/, \/-}

private[http] object JwtParties {
  import com.digitalasset.canton.http.EndpointsCompanion.{Error, Unauthorized}

  // security check for readAs; we delegate the remainder to
  // the participant's check that the JWT itself is valid
  def ensureReadAsAllowedByJwt(
      readAs: Option[NonEmptyList[http.Party]],
      jwtPayload: JwtPayload,
  ): Error \/ Unit = {
    val disallowedParties: Set[http.Party] =
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
      meta: Option[http.CommandMeta.IgnoreDisclosed],
      jwtPayload: JwtWritePayload,
  ): http.PartySet = {
    val actAs = meta.flatMap(_.actAs) getOrElse jwtPayload.submitter
    val readAs = meta.flatMap(_.readAs) getOrElse jwtPayload.readAs
    actAs.toSet1 ++ readAs
  }
}
