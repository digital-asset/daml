package com.digitalasset.daml.lf.transaction

import cats.Semigroup
import com.daml.lf.data.Ref.PackageId

case class PackageRequirements(checkOnly: Set[PackageId], vetted: Set[PackageId]) {
  lazy val isEmpty: Boolean = checkOnly.isEmpty && vetted.isEmpty

  def subgroupOf(other: PackageRequirements): Boolean =
    checkOnly.subsetOf(other.checkOnly) && vetted.subsetOf(other.vetted)
}

object PackageRequirements {

  val empty: PackageRequirements = PackageRequirements(Set.empty, Set.empty)

  implicit val packagesUsedSemigroup: Semigroup[PackageRequirements] = Semigroup.instance {
    (x, y) =>
      val vetted = x.vetted ++ y.vetted
      val checkOnly = (x.checkOnly ++ y.checkOnly) -- vetted
      PackageRequirements(
        checkOnly = checkOnly,
        vetted = vetted,
      )
  }

  def checkOnly(packageIds: PackageId*): PackageRequirements =
    PackageRequirements(checkOnly = packageIds.toSet, vetted = Set.empty)

  def vetted(packageIds: PackageId*): PackageRequirements =
    PackageRequirements(checkOnly = Set.empty, vetted = packageIds.toSet)
}
