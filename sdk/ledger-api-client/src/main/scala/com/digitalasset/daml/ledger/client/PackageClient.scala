// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client

import com.daml.ledger.api.v2.package_service.PackageServiceGrpc.PackageServiceStub
import com.daml.ledger.api.v2.package_service.{ListPackagesRequest, ListPackagesResponse}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

final class PackageClient(
    service: PackageServiceStub,
    getDefaultToken: () => Option[String] = () => None,
) {

  def listPackages(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[ListPackagesResponse] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .listPackages(ListPackagesRequest())
}
