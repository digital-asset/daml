// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.data.Ref.*
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver.AuthorityResponse
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait AuthorityResolver {
  def resolve(request: AuthorityResolver.AuthorityRequest)(implicit
      traceContext: TraceContext
  ): Future[AuthorityResponse]
}

object AuthorityResolver {
  sealed trait AuthorityResponse
  object AuthorityResponse {
    final case object Authorized extends AuthorityResponse
    final case class MissingAuthorisation(parties: Set[Party]) extends AuthorityResponse
  }

  final case class AuthorityRequest(
      holding: Set[Party],
      requesting: Set[Party],
      domainId: Option[DomainId],
  )

  def apply(): AuthorityResolver = new TopologyUnawareAuthorityResolver

  class TopologyUnawareAuthorityResolver extends AuthorityResolver {
    override def resolve(request: AuthorityRequest)(implicit
        traceContext: TraceContext
    ): Future[AuthorityResponse] =
      Future.successful(AuthorityResponse.MissingAuthorisation(request.requesting))
  }

}
