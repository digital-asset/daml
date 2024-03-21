// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.daml.jwt.domain.Jwt
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.admin.GetPackageResponse
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.util.ProtobufByteStrings
import com.digitalasset.canton.ledger.api.domain as LedgerApiDomain

import scala.concurrent.{ExecutionContext, Future}

class PackageManagementService(
    listKnownPackagesFn: LedgerClientJwt.ListPackages,
    getPackageFn: LedgerClientJwt.GetPackage,
    uploadDarFileFn: LedgerClientJwt.UploadDarFile,
)(implicit ec: ExecutionContext, mat: Materializer) {

  def listPackages(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Seq[String]] =
    listKnownPackagesFn(jwt, ledgerId)(lc).map(_.packageIds)

  def getPackage(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId, packageId: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[GetPackageResponse] =
    getPackageFn(jwt, ledgerId, packageId)(lc).map(admin.GetPackageResponse.fromLedgerApi)

  def uploadDarFile(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      source: Source[ByteString, NotUsed],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Unit] =
    uploadDarFileFn(jwt, ledgerId, ProtobufByteStrings.readFrom(source))(lc)
}
