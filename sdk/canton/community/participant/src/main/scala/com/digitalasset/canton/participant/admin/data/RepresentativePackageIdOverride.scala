// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.implicits.toTraverseOps
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfPackageId, LfPackageName}
import com.digitalasset.daml.lf.value.Value.ContractId

/** Defines override mappings for assigning representative package IDs to contracts upon ACS import.
  * See [[com.digitalasset.canton.admin.participant.v30.RepresentativePackageIdOverride]]
  */
final case class RepresentativePackageIdOverride(
    contractOverride: Map[ContractId, LfPackageId],
    packageIdOverride: Map[LfPackageId, LfPackageId],
    packageNameOverride: Map[LfPackageName, LfPackageId],
) extends PrettyPrinting {
  def toProtoV30: v30.RepresentativePackageIdOverride =
    v30.RepresentativePackageIdOverride(
      // Apparently structurally trivial mappings, but upcasting from domain types to Strings
      contractOverride = contractOverride.map { case (k, v) => k.coid -> v },
      packageIdOverride = packageIdOverride.map { case (k, v) => k -> v },
      packageNameOverride = packageNameOverride.map { case (k, v) => k -> v },
    )

  override protected def pretty: Pretty[RepresentativePackageIdOverride] =
    prettyOfClass(
      paramIfNonEmpty("contractOverride", _.contractOverride),
      paramIfNonEmpty("packageIdOverride", _.packageIdOverride),
      paramIfNonEmpty("packageNameOverride", _.packageNameOverride),
    )
}

object RepresentativePackageIdOverride {
  val NoOverride: RepresentativePackageIdOverride =
    RepresentativePackageIdOverride(Map.empty, Map.empty, Map.empty)

  def fromProtoV30(
      representativePackageIdOverrideP: v30.RepresentativePackageIdOverride
  ): ParsingResult[RepresentativePackageIdOverride] = for {
    contractOverride <- representativePackageIdOverrideP.contractOverride.toSeq.traverse {
      case (contractId, packageId) =>
        for {
          cid <- ProtoConverter.parseLfContractId(contractId)
          pkgId <- ProtoConverter.parsePackageId(packageId)
        } yield cid -> pkgId
    }
    packageIdOverride <- representativePackageIdOverrideP.packageIdOverride.toSeq.traverse {
      case (from, to) =>
        for {
          fromPkgId <- ProtoConverter.parsePackageId(from)
          toPkgId <- ProtoConverter.parsePackageId(to)
        } yield fromPkgId -> toPkgId
    }
    packageNameOverride <- representativePackageIdOverrideP.packageNameOverride.toSeq.traverse {
      case (pkgName, toPkgIdProto) =>
        for {
          fromPkgName <- ProtoConverter.parsePackageName(pkgName)
          toPkgId <- ProtoConverter.parsePackageId(toPkgIdProto)
        } yield fromPkgName -> toPkgId
    }
  } yield new RepresentativePackageIdOverride(
    contractOverride = contractOverride.toMap,
    packageIdOverride = packageIdOverride.toMap,
    packageNameOverride = packageNameOverride.toMap,
  )
}
