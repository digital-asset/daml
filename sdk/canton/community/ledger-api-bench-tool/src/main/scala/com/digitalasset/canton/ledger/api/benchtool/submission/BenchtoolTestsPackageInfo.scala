// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.digitalasset.canton.ledger.api.benchtool.infrastructure.TestDars
import com.digitalasset.daml.lf.data.Ref

final case class BenchtoolTestsPackageInfo private (packageRef: Ref.PackageRef)

object BenchtoolTestsPackageInfo {

  val StaticDefault: BenchtoolTestsPackageInfo =
    BenchtoolTestsPackageInfo(packageRef = TestDars.benchtoolDarPackageRef)
}
