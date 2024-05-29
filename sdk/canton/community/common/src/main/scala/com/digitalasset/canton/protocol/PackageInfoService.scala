// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPackageVersion}
import slick.jdbc.GetResult

import scala.concurrent.Future

/** @param packageId         the unique identifier for the package
  * @param sourceDescription an informal human readable description of what the package contains
  */
final case class PackageDescription(
    packageId: LfPackageId,
    sourceDescription: String256M,
    packageName: Option[LfPackageName],
    packageVersion: Option[LfPackageVersion],
)

object PackageDescription {

  import com.digitalasset.canton.resource.DbStorage.Implicits.*

  implicit val getResult: GetResult[PackageDescription] =
    GetResult
      .createGetTuple4(
        GetResult[LfPackageId],
        GetResult[String256M],
        GetResult[Option[LfPackageName]],
        GetResult[Option[LfPackageVersion]],
      )
      .andThen { case (packageId, sourceDescription, pkgName, pkgVersion) =>
        PackageDescription(packageId, sourceDescription, pkgName, pkgVersion)
      }
}

trait PackageInfoService {

  def getDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]]

}
