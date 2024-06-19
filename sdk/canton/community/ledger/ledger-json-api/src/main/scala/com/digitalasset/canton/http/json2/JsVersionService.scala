// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.daml.ledger.api.v2.experimental_features
import com.daml.ledger.api.v2.version_service
import com.digitalasset.canton.http.json2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.generic.auto.*

import scala.concurrent.{ExecutionContext, Future}


class JsVersionService(versionClient: VersionClient)(implicit
    val executionContext: ExecutionContext
) extends Endpoints {
  import JsVersionServiceCodecs.*
  private val version = baseEndpoint.in("version")

  def endpoints() = List(
    json(
      version.get
        .description("Get the version details of the participant node"),
      getVersion,
    )
  )
  private def getVersion(
      caller: CallerContext
  ): TracedInput[Unit] => Future[Either[JsCantonError, version_service.GetLedgerApiVersionResponse]] =
    tracedInput =>
      versionClient
        .serviceStub(caller.token())(tracedInput.traceContext)
        .getLedgerApiVersion(version_service.GetLedgerApiVersionRequest()).toRight
}

object JsVersionServiceCodecs {
  implicit val est: Codec[experimental_features.ExperimentalStaticTime] = deriveCodec
  implicit val ef: Codec[experimental_features.ExperimentalFeatures] = deriveCodec
  implicit val umf: Codec[version_service.UserManagementFeature] = deriveCodec
  implicit val pmf: Codec[version_service.PartyManagementFeature] = deriveCodec
  implicit val fd: Codec[version_service.FeaturesDescriptor] = deriveCodec
  implicit val glavr: Codec[version_service.GetLedgerApiVersionResponse] = deriveCodec
}
