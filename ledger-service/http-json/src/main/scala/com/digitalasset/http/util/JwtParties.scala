// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package util

import domain.{JwtPayload, JwtWritePayload, JwtPayloadG}
import com.daml.scalautil.nonempty.NonEmptyReturningOps._
import scalaz.{\/, \/-, -\/, NonEmptyList}
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._

private[http] object JwtParties {
  import EndpointsCompanion.{Error, Unauthorized}

  private[this] def ensurePartiesAllowedByJwt(
      parties: Option[Set[domain.Party]],
      jwtPayload: JwtPayloadG,
  ): Error \/ Unit = {
    val disallowedParties: Set[domain.Party] =
      parties.cata((_.filterNot(jwtPayload.parties)), Set.empty)
    if (disallowedParties.isEmpty) \/-(())
    else {
      val err =
        s"$EnsureReadAsDisallowedError: ${disallowedParties mkString ", "}"
      -\/(Unauthorized(err))
    }
  }

  private[util] val EnsureReadAsDisallowedError = "Queried parties not allowed by given JWT token"

  // security check for readAs; we delegate the remainder to
  // the participant's check that the JWT itself is valid
  def ensureReadAsAllowedByJwt(
      readAs: Option[NonEmptyList[domain.Party]],
      jwtPayload: JwtPayload,
  ): Error \/ Unit =
    ensurePartiesAllowedByJwt(readAs map (_.toSet), jwtPayload)

  def resolveRefParties(
      meta: Option[domain.CommandMeta],
      jwtPayload: JwtWritePayload,
  ): Error \/ domain.PartySet = {
    val actAs = meta.flatMap(_.actAs) getOrElse jwtPayload.submitter
    val readAs = meta.flatMap(_.readAs) getOrElse jwtPayload.readAs
    val result = actAs.toSet1 ++ readAs
    ensurePartiesAllowedByJwt(Some(result), jwtPayload) as result
  }
}
