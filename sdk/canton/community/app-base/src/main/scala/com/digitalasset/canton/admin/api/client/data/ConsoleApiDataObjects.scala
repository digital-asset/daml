// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.participant.v30 as participantAdminV30
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.typesafe.scalalogging.LazyLogging

final case class ListConnectedSynchronizersResult(
    synchronizerAlias: SynchronizerAlias,
    synchronizerId: PhysicalSynchronizerId,
    healthy: Boolean,
)

object ListConnectedSynchronizersResult {

  def fromProtoV30(
      value: participantAdminV30.ListConnectedSynchronizersResponse.Result
  ): ParsingResult[ListConnectedSynchronizersResult] = {
    val participantAdminV30.ListConnectedSynchronizersResponse.Result(
      synchronizerAlias,
      synchronizerId,
      healthy,
    ) =
      value
    for {
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        synchronizerId,
        "physical_synchronizer_id",
      )
      synchronizerAlias <- SynchronizerAlias.fromProtoPrimitive(synchronizerAlias)

    } yield ListConnectedSynchronizersResult(
      synchronizerAlias = synchronizerAlias,
      synchronizerId = synchronizerId,
      healthy = healthy,
    )
  }
}

final case class DarDescription(
    mainPackageId: String,
    name: String,
    version: String,
    description: String,
)
object DarDescription extends LazyLogging {

  def fromProtoV30(
      value: participantAdminV30.DarDescription
  ): ParsingResult[DarDescription] = {
    val participantAdminV30.DarDescription(
      mainPackageId,
      name,
      version,
      description,
    ) =
      value
    Right(
      DarDescription(
        mainPackageId = mainPackageId,
        name = name,
        version = version,
        description = description,
      )
    )
  }
}

final case class DarContents(
    description: DarDescription,
    packages: Seq[PackageDescription],
) extends PrettyPrinting {
  override protected def pretty: Pretty[DarContents] = prettyOfClass(
    param("main", _.description.mainPackageId.readableHash),
    param("name", _.description.description.unquoted),
    param("version", _.description.version.unquoted),
    param("description", _.description.description.unquoted),
    param(
      "packages",
      x =>
        x.packages
          .filter(y => y.packageId != x.description.mainPackageId)
          .map(_.packageId.readableHash),
    ),
  )
}

object DarContents {

  def fromProtoV30(
      value: participantAdminV30.GetDarContentsResponse
  ): ParsingResult[DarContents] = {
    val participantAdminV30.GetDarContentsResponse(
      descriptionP,
      packagesP,
    ) =
      value
    for {
      description <- ProtoConverter.parseRequired(DarDescription.fromProtoV30, "main", descriptionP)
      dependencies <- packagesP.traverse(PackageDescription.fromProto)
    } yield DarContents(description, dependencies)
  }
}

final case class PackageDescription(
    packageId: String,
    name: String,
    version: String,
    uploadedAt: CantonTimestamp,
    packageSize: Int,
) extends PrettyPrinting {
  override protected def pretty: Pretty[PackageDescription] = prettyOfClass(
    param("packageId", _.packageId.readableHash),
    param("name", _.name.unquoted),
    param("version", _.version.unquoted),
    param("uploadedAt", _.uploadedAt),
    param("size", _.packageSize),
  )
}

object PackageDescription {
  def fromProto(
      proto: participantAdminV30.PackageDescription
  ): ParsingResult[PackageDescription] = {
    val participantAdminV30.PackageDescription(packageId, name, version, uploadedAt, packageSize) =
      proto
    for {
      ts <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "uploadedAt",
        uploadedAt,
      )

    } yield PackageDescription(
      packageId = packageId,
      name = name,
      version = version,
      uploadedAt = ts,
      packageSize = packageSize,
    )
  }

  final case class PackageContents(
      description: PackageDescription,
      modules: Set[String],
      isUtilityPackage: Boolean,
      languageVersion: String,
  )

}
