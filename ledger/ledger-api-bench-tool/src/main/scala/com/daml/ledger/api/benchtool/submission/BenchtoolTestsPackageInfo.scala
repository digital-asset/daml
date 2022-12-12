// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.test.benchtool.Foo.Foo1
import scalaz.syntax.tag._

case class BenchtoolTestsPackageInfo(
    packageId: String
)

object BenchtoolTestsPackageInfo {
  val BenchtoolTestsPackageName = "benchtool-tests"

  // The packageId obtained from the compiled Scala bindings
  val StaticDefault: BenchtoolTestsPackageInfo =
    BenchtoolTestsPackageInfo(packageId = Foo1.id.unwrap.packageId)

}
