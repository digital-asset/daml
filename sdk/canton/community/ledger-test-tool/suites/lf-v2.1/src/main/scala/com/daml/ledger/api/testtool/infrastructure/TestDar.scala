// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

sealed trait TestDar extends Product with Serializable { val path: String }

object TestDar {
  private val v21Dars = List(
    ModelTestDar,
    SemanticTestDar,
    OngoingStreamPackageUploadTestDar,
    PackageManagementTestDar,
    Carbonv1TestDar,
    Carbonv2TestDar,
  ).map(_.path)

  private val v2devDars =
    v21Dars ++ Seq(ExperimentalTestDar).map(_.path)

  val paths: Map[String, List[String]] = Map(
    "2.1" -> v21Dars,
    "2.dev" -> v2devDars,
  )
}

case object ExperimentalTestDar extends TestDar { val path = "experimental-tests-3.1.0.dar" }
case object ModelTestDar extends TestDar { val path = "model-tests-3.1.0.dar" }
case object SemanticTestDar extends TestDar { val path = "semantic-tests-3.1.0.dar" }
case object OngoingStreamPackageUploadTestDar extends TestDar {
  val path = "ongoing-stream-package-upload-tests-3.1.0.dar"
}
case object PackageManagementTestDar extends TestDar {
  val path = "package-management-tests-3.1.0.dar"
}
case object Carbonv1TestDar extends TestDar { val path = "carbonv1-tests-3.1.0.dar" }
case object Carbonv2TestDar extends TestDar { val path = "carbonv2-tests-3.1.0.dar" }
case object UpgradeTestDar1_0_0 extends TestDar { val path = "upgrade-tests-1.0.0.dar" }
case object UpgradeTestDar2_0_0 extends TestDar { val path = "upgrade-tests-2.0.0.dar" }
case object UpgradeTestDar3_0_0 extends TestDar { val path = "upgrade-tests-3.0.0.dar" }
case object UpgradeFetchTestDar1_0_0 extends TestDar { val path = "upgrade-fetch-tests-1.0.0.dar" }
case object UpgradeFetchTestDar2_0_0 extends TestDar { val path = "upgrade-fetch-tests-2.0.0.dar" }
case object UpgradeIfaceDar extends TestDar { val path = "upgrade-iface-tests-3.1.0.dar" }

case object VettingMainDar_1_0_0 extends TestDar { val path = "vetting-main-1.0.0.dar" }
case object VettingMainDar_2_0_0 extends TestDar { val path = "vetting-main-2.0.0.dar" }
case object VettingMainDar_Split_Lineage_2_0_0 extends TestDar {
  val path = "vetting-main-split-lineage-2.0.0.dar"
}
case object VettingMainDar_3_0_0_Incompatible extends TestDar {
  val path = "vetting-main-3.0.0.dar"
}
case object VettingDepDar extends TestDar { val path = "vetting-dep-1.0.0.dar" }
case object VettingAltDar extends TestDar { val path = "vetting-alt-1.0.0.dar" }
