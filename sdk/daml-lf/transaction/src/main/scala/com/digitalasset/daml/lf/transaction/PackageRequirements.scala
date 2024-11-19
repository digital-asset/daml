// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import cats.Semigroup
import com.daml.lf.data.Ref.PackageId

/** Classifies the packages required for fully validating a Daml transaction into
  *
  * * check-only - Packages needed solely for replaying the creation of the input contracts for consistency checking reasons
  *
  * * vetted - Packages needed for evaluating the action nodes of the transaction
  */
case class PackageRequirements(checkOnly: Set[PackageId], vetted: Set[PackageId]) {
  lazy val isEmpty: Boolean = checkOnly.isEmpty && vetted.isEmpty

  // Ensure the vetted and check-only sets are disjoint
  // since a vetted package implies it can be used for consistency checking on replay create as well
  def normalized: PackageRequirements = copy(vetted = vetted, checkOnly = checkOnly -- vetted)
}

object PackageRequirements {
  val empty: PackageRequirements = PackageRequirements(Set.empty, Set.empty)

  implicit val packagesUsedSemigroup: Semigroup[PackageRequirements] = Semigroup.instance {
    (x, y) =>
      PackageRequirements(
        checkOnly = x.checkOnly ++ y.checkOnly,
        vetted = x.vetted ++ y.vetted,
      )
  }

  def checkOnly(packageIds: PackageId*): PackageRequirements =
    PackageRequirements(checkOnly = packageIds.toSet, vetted = Set.empty)

  def vetted(packageIds: PackageId*): PackageRequirements =
    PackageRequirements(checkOnly = Set.empty, vetted = packageIds.toSet)
}
