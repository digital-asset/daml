// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import sttp.tapir.server.pekkohttp.PekkoHttpServerInterpreter

import scala.concurrent.ExecutionContext

class V2Routes(
    identityProviderService: JsIdentityProviderService,
    meteringService: JsMeteringService,
    packageService: JsPackageService,
    partyManagementService: JsPartyManagementService,
    userManagementService: JsUserManagementService,
    versionService: JsVersionService,
    val loggerFactory: NamedLoggerFactory,
)(ec: ExecutionContext)
    extends Endpoints
    with NamedLogging {

  val v2Routes: Route =
    PekkoHttpServerInterpreter()(ec).toRoute(
      versionService.endpoints()
        ++ packageService.endpoints()
        ++ partyManagementService.endpoints()
        ++ userManagementService.endpoints()
        ++ identityProviderService.endpoints()
        ++ meteringService.endpoints()
    )
}

object V2Routes {
  def apply(
      ledgerClient: LedgerClient,
      executionContext: ExecutionContext,
      materializer: Materializer,
      loggerFactory: NamedLoggerFactory,
  ): V2Routes = {
    implicit val ec: ExecutionContext = executionContext

    val versionService = new JsVersionService(ledgerClient.v2.versionClient)

    val partyManagementService = new JsPartyManagementService(ledgerClient.partyManagementClient, loggerFactory)

    val packageService =
      new JsPackageService(ledgerClient.v2.packageService, ledgerClient.packageManagementClient)(
        executionContext,
        materializer,
      )

    val userManagementService =
      new JsUserManagementService(ledgerClient.userManagementClient, loggerFactory)
    val meteringService = new JsMeteringService(ledgerClient.meteringReportClient)

    val identityProviderService = new JsIdentityProviderService(
      ledgerClient.identityProviderConfigClient,
      loggerFactory,
    )

    new V2Routes(
      identityProviderService,
      meteringService,
      packageService,
      partyManagementService,
      userManagementService,
      versionService,
      loggerFactory,
    )(executionContext)
  }

}
