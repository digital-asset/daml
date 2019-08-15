// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.{v1 => lav1}
import org.scalacheck.Gen

object Generators {
  def genApiIdentifier: Gen[lav1.value.Identifier] =
    for {
      p <- Gen.identifier
      m <- Gen.identifier
      e <- Gen.identifier
    } yield lav1.value.Identifier(packageId = p, moduleName = m, entityName = e)

  def genDomainTemplateId: Gen[domain.TemplateId.RequiredPkg] =
    genApiIdentifier.map(domain.TemplateId.fromLedgerApi)

  def genDomainTemplateIdO[A](implicit ev: PackageIdGen[A]): Gen[domain.TemplateId[A]] =
    for {
      p <- ev.gen
      m <- Gen.identifier
      e <- Gen.identifier
    } yield domain.TemplateId(p, m, e)

  def nonEmptySet[A](gen: Gen[A]): Gen[Set[A]] = Gen.nonEmptyListOf(gen).map(_.toSet)

  // Generate Identifiers with unique packageId values, but the same moduleName and entityName.
  def genDuplicateApiIdentifiers: Gen[List[lav1.value.Identifier]] =
    for {
      id0 <- genApiIdentifier
      otherPackageIds <- nonEmptySet(Gen.identifier.filter(x => x != id0.packageId))
    } yield id0 :: otherPackageIds.map(a => id0.copy(packageId = a)).toList

  def genDuplicateDomainTemplateIdR: Gen[List[domain.TemplateId.RequiredPkg]] =
    genDuplicateApiIdentifiers.map(xs => xs.map(domain.TemplateId.fromLedgerApi))

  trait PackageIdGen[A] {
    def gen: Gen[A]
  }

  implicit object RequiredPackageIdGen extends PackageIdGen[String] {
    override def gen: Gen[String] = Gen.identifier
  }

  implicit object NoPackageIdGen extends PackageIdGen[Unit] {
    override def gen: Gen[Unit] = Gen.const(())
  }

  implicit object OptionalPackageIdGen extends PackageIdGen[Option[String]] {
    override def gen: Gen[Option[String]] = Gen.option(RequiredPackageIdGen.gen)
  }
}
