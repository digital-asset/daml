// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.traverse.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, PackageName, PackageVersion}
import com.digitalasset.canton.ledger.api.validation.ValidateUpgradingPackageResolutions.ValidatedCommandPackageResolutionsSnapshot
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataStore
import com.google.common.annotations.VisibleForTesting
import io.grpc.StatusRuntimeException

trait ValidateUpgradingPackageResolutions {
  def apply(
      userPackageIdPreferences: Seq[String]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[
    StatusRuntimeException,
    ValidatedCommandPackageResolutionsSnapshot,
  ]
}

class ValidateUpgradingPackageResolutionsImpl(packageMetadataStore: PackageMetadataStore)
    extends ValidateUpgradingPackageResolutions {
  def apply(
      rawUserPackageIdPreferences: Seq[String]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[
    StatusRuntimeException,
    ValidatedCommandPackageResolutionsSnapshot,
  ] = {
    val packageMetadataSnapshot = packageMetadataStore.getSnapshot
    val packageResolutionMapSnapshot = packageMetadataSnapshot.getUpgradablePackageMap
    val participantPackagePreferenceMapSnapshot =
      packageMetadataSnapshot.getUpgradablePackagePreferenceMap

    for {
      userPackageIdPreferences <- rawUserPackageIdPreferences
        .traverse(Ref.PackageId.fromString)
        .left
        .map(err =>
          invalidArgument(
            s"package_id_selection_preference parsing failed with `$err`. The package_id_selection_preference field must contain non-empty and valid package ids"
          )
        )
      userPackagePreferenceMap <- userPackageIdPreferences
        .traverse(pkgId =>
          packageResolutionMapSnapshot
            .get(pkgId)
            .map(_._1 -> pkgId)
            .toRight(invalidArgument(s"user-specified pkg id ($pkgId) could not be found"))
        )
      validatedUserPackagePreferenceMap <-
        userPackagePreferenceMap.foldLeft(
          Right(Map.empty): Either[StatusRuntimeException, Map[PackageName, PackageId]]
        ) {
          case (Right(acc), (packageName, userPref)) =>
            acc.get(packageName) match {
              case Some(existing) =>
                Left(
                  invalidArgument(
                    s"duplicate preference for package-name $packageName: $existing vs $userPref"
                  )
                )
              case None => Right(acc.updated(packageName, userPref))
            }
          case (Left(err), _) => Left(err)
        }
    } yield {
      val submissionPackagePreferenceSet =
        (participantPackagePreferenceMapSnapshot ++ validatedUserPackagePreferenceMap).values
          // It's fine provided that we disallow uploading of unrelated package-ids for the same package-name
          .toSet
      ValidatedCommandPackageResolutionsSnapshot(
        packageResolutionMapSnapshot,
        submissionPackagePreferenceSet,
      )
    }
  }
}

object ValidateUpgradingPackageResolutions {
  final case class ValidatedCommandPackageResolutionsSnapshot(
      packageMap: Map[PackageId, (PackageName, PackageVersion)],
      packagePreferenceSet: Set[PackageId],
  )

  def apply(packageMetadataStore: PackageMetadataStore): ValidateUpgradingPackageResolutions =
    new ValidateUpgradingPackageResolutionsImpl(packageMetadataStore)

  @VisibleForTesting
  val Empty: ValidateUpgradingPackageResolutions =
    new ValidateUpgradingPackageResolutions {
      override def apply(userPackageIdPreferences: Seq[String])(implicit
          contextualizedErrorLogger: ContextualizedErrorLogger
      ): Either[StatusRuntimeException, ValidatedCommandPackageResolutionsSnapshot] =
        Right(
          ValidatedCommandPackageResolutionsSnapshot(
            Map.empty,
            Set.empty,
          )
        )
    }
}
