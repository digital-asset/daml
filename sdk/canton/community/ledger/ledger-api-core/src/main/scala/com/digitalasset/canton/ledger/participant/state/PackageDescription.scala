// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.data.CantonTimestamp
import slick.jdbc.GetResult
import slick.jdbc.GetResult.GetInt

/** @param packageId
  *   the unique identifier for the package
  * @param name
  *   name of the package (from package metadata])
  * @param version
  *   version of package (from package metadata)
  * @param uploadedAt
  *   The package upload timestamp
  * @param packageSize
  *   The LF archive protobuf-serialized size in bytes
  */
final case class PackageDescription(
    packageId: LfPackageId,
    name: String255,
    version: String255,
    uploadedAt: CantonTimestamp,
    packageSize: Int,
)

object PackageDescription {

  import com.digitalasset.canton.resource.DbStorage.Implicits.*
  implicit val getResult: GetResult[PackageDescription] = GetResult { r =>
    val packageId = r.<<[LfPackageId]
    val name = r.<<[String255]
    val version = r.<<[String255]
    val uploadedAt = r.<<[CantonTimestamp]
    val packageSize = r.<<[Int]
    PackageDescription(
      packageId = packageId,
      name = name,
      version = version,
      uploadedAt = uploadedAt,
      packageSize = packageSize,
    )
  }
}
