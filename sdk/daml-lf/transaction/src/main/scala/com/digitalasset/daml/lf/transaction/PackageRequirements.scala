package com.digitalasset.daml.lf.transaction

import cats.Semigroup
import com.daml.lf.data.Ref.PackageId

case class PackageRequirements(knownOnly: Set[PackageId], vetted: Set[PackageId]) {
  lazy val isEmpty: Boolean = knownOnly.isEmpty && vetted.isEmpty

  def subgroupOf(other: PackageRequirements): Boolean =
    knownOnly.subsetOf(other.knownOnly) && vetted.subsetOf(other.vetted)
}

object PackageRequirements {

  val empty: PackageRequirements = PackageRequirements(Set.empty, Set.empty)

  implicit val packagesUsedSemigroup: Semigroup[PackageRequirements] = Semigroup.instance {
    (x, y) =>
      val vetted = x.vetted ++ y.vetted
      val known = (x.knownOnly ++ y.knownOnly) -- vetted
      PackageRequirements(
        knownOnly = known,
        vetted = vetted,
      )
  }

  def knownOnly(singleton: PackageId): PackageRequirements =
    PackageRequirements(knownOnly = Set(singleton), vetted = Set.empty)

  def vetted(singleton: PackageId): PackageRequirements =
    PackageRequirements(knownOnly = Set.empty, vetted = Set(singleton))
}
