// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.version

import com.daml.ledger.api.v2.version_service.VersionServiceGrpc.VersionServiceStub
import com.daml.ledger.api.v2.version_service.{FeaturesDescriptor, GetLedgerApiVersionRequest}
import com.digitalasset.canton.ledger.api.domain.Feature
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final class VersionClient(service: VersionServiceStub) {
  def getApiVersion(
      token: Option[String] = None
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[String] =
    LedgerClient
      .stubWithTracing(service, token)
      .getLedgerApiVersion(
        new GetLedgerApiVersionRequest()
      )
      .map(_.version)
}

object VersionClient {
  // see also com.digitalasset.canton.platform.apiserver.services.ApiVersionService.featuresDescriptor
  def fromProto(featuresDescriptor: FeaturesDescriptor): Seq[Feature] =
    featuresDescriptor match {
      // Note that we do not expose experimental features here, as they are used for internal testing only
      // and do not have backwards compatibility guarantees. (They should probably be named 'internalFeatures' ;-)
      case FeaturesDescriptor(userManagement, _, _) =>
        (userManagement.toList map (_ => Feature.UserManagement))
    }
}
