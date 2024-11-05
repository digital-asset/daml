// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.Jwt
import com.digitalasset.canton.http.{PackageService, WebsocketConfig}
import com.digitalasset.canton.http.util.Logging.instanceUUIDLogCtx
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.DamlDefinitionsView
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import com.digitalasset.canton.ledger.participant.state.WriteService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import sttp.tapir.server.pekkohttp.PekkoHttpServerInterpreter
import com.digitalasset.daml.lf.data.Ref.IdString

import scala.concurrent.ExecutionContext

class V2Routes(
    commandService: JsCommandService,
    eventService: JsEventService,
    identityProviderService: JsIdentityProviderService,
    meteringService: JsMeteringService,
    packageService: JsPackageService,
    partyManagementService: JsPartyManagementService,
    stateService: JsStateService,
    updateService: JsUpdateService,
    userManagementService: JsUserManagementService,
    versionService: JsVersionService,
    metadataServiceIfEnabled: Option[JsDamlDefinitionsService],
    versionClient: VersionClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends Endpoints
    with NamedLogging {
  private val serverEndpoints =
    commandService.endpoints() ++ eventService.endpoints() ++ versionService
      .endpoints() ++ packageService.endpoints() ++ partyManagementService
      .endpoints() ++ stateService.endpoints() ++ updateService.endpoints() ++ userManagementService
      .endpoints() ++ identityProviderService.endpoints() ++ meteringService
      .endpoints() ++ metadataServiceIfEnabled.toList.flatMap(_.endpoints())

  private val docs =
    new JsApiDocsService(versionClient, serverEndpoints.map(_.endpoint), loggerFactory)

  val v2Routes: Route =
    PekkoHttpServerInterpreter()(ec).toRoute(serverEndpoints)

  val docsRoute = PekkoHttpServerInterpreter()(ec).toRoute(docs.endpoints())
}

object V2Routes {
  def apply(
      ledgerClient: LedgerClient,
      packageService: PackageService,
      metadataServiceEnabled: Boolean,
      writeService: WriteService,
      executionContext: ExecutionContext,
      materializer: Materializer,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      esf: ExecutionSequencerFactory,
      ws: WebsocketConfig,
  ): V2Routes = {
    implicit val ec: ExecutionContext = executionContext

    def fetchSignatures(token: Option[String]) = for {
      _ <- instanceUUIDLogCtx { implicit lc =>
        packageService.reload(
          Jwt(token.getOrElse(""))
        )
      }
    } yield packageService.packageStore
      .map { case (k, v) =>
        (IdString.PackageId.assertFromString(k), v.pack)
      }

    val schemaProcessors = new SchemaProcessors(fetchSignatures)
    val protocolConverters = new ProtocolConverters(schemaProcessors)
    val commandService =
      new JsCommandService(ledgerClient, protocolConverters, loggerFactory)

    val eventService =
      new JsEventService(ledgerClient, protocolConverters, loggerFactory)
    val versionService = new JsVersionService(ledgerClient.versionClient, loggerFactory)

    val stateService =
      new JsStateService(ledgerClient, protocolConverters, loggerFactory)
    val partyManagementService =
      new JsPartyManagementService(ledgerClient.partyManagementClient, loggerFactory)

    val jsPackageService =
      new JsPackageService(
        ledgerClient.packageService,
        ledgerClient.packageManagementClient,
        loggerFactory,
      )(
        executionContext,
        materializer,
      )

    val updateService =
      new JsUpdateService(ledgerClient, protocolConverters, loggerFactory)

    val userManagementService =
      new JsUserManagementService(ledgerClient.userManagementClient, loggerFactory)
    val meteringService = new JsMeteringService(ledgerClient.meteringReportClient, loggerFactory)

    val identityProviderService = new JsIdentityProviderService(
      ledgerClient.identityProviderConfigClient,
      loggerFactory,
    )

    val damlDefinitionsServiceIfEnabled = Option.when(metadataServiceEnabled) {
      val damlDefinitionsService =
        new DamlDefinitionsView(writeService.getPackageMetadataSnapshot(_))
      new JsDamlDefinitionsService(damlDefinitionsService, loggerFactory)
    }

    new V2Routes(
      commandService,
      eventService,
      identityProviderService,
      meteringService,
      jsPackageService,
      partyManagementService,
      stateService,
      updateService,
      userManagementService,
      versionService,
      damlDefinitionsServiceIfEnabled,
      ledgerClient.versionClient,
      loggerFactory,
    )(executionContext)
  }
}
