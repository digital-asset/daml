// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.daml.http.util.Logging.{CorrelationID, RequestID}
import com.daml.http.util.ProtobufByteStrings
import com.daml.jwt.domain.Jwt
import com.daml.logging.LoggingContextOf

import scala.concurrent.{ExecutionContext, Future}

class PackageManagementService(
    listKnownPackagesFn: LedgerClientJwt.ListPackages,
    getPackageFn: LedgerClientJwt.GetPackage,
    uploadDarFileFn: LedgerClientJwt.UploadDarFile,
)(implicit ec: ExecutionContext, mat: Materializer) {

  def listPackages(jwt: Jwt)(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Future[Seq[String]] =
    listKnownPackagesFn(jwt)(lc).map(_.packageIds)

  def getPackage(jwt: Jwt, packageId: String)(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Future[admin.GetPackageResponse] =
    getPackageFn(jwt, packageId)(lc).map(admin.GetPackageResponse.fromLedgerApi)

  def uploadDarFile(jwt: Jwt, source: Source[ByteString, NotUsed])(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Future[Unit] =
    uploadDarFileFn(jwt, ProtobufByteStrings.readFrom(source))(lc)
}
