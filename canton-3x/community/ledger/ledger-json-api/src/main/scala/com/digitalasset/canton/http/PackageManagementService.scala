// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import scala.concurrent.{ExecutionContext, Future}

class PackageManagementService(
    listKnownPackagesFn: LedgerClientJwt.ListPackages,
    getPackageFn: LedgerClientJwt.GetPackage,
    uploadDarFileFn: LedgerClientJwt.UploadDarFile,
)(implicit ec: ExecutionContext, mat: Materializer) {

  def listPackages(jwt: Jwt)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Seq[String]] =
    listKnownPackagesFn(jwt)(lc).map(_.packageIds)

  def getPackage(jwt: Jwt, packageId: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[GetPackageResponse] =
    getPackageFn(jwt, packageId)(lc).map(admin.GetPackageResponse.fromLedgerApi)

  def uploadDarFile(
      jwt: Jwt,
      source: Source[ByteString, NotUsed],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Unit] =
    uploadDarFileFn(jwt, ProtobufByteStrings.readFrom(source))(lc)
}
