// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.integrationtest.CantonFixture.{readDar, CompiledDar}
import com.daml.lf.speedy.SValue._
import java.nio.file.Paths
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class PackageUpgradesIT
    extends AsyncWordSpec
    with AbstractScriptTest
    with Inside
    with Matchers {
  final override protected lazy val devMode = true
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  val coinV1DarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-v1.dar"))
  val coinV2DarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-v2.dar"))
  val coinV3DarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-v3.dar"))
  val coinUpgradeV1V2DarPath =
    BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-upgrade-v1-v2.dar"))
  val coinUpgradeV1V3DarPath =
    BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-upgrade-v1-v3.dar"))
  val coinV1Dar: CompiledDar = readDar(coinV1DarPath, Runner.compilerConfig)
  val coinV2Dar: CompiledDar = readDar(coinV2DarPath, Runner.compilerConfig)
  val coinUpgradeV1V2Dar: CompiledDar = readDar(coinUpgradeV1V2DarPath, Runner.compilerConfig)
  val coinUpgradeV1V3Dar: CompiledDar = readDar(coinUpgradeV1V3DarPath, Runner.compilerConfig)

  override protected lazy val darFiles = List(
    coinV1DarPath,
    coinV2DarPath,
    coinV3DarPath,
    coinUpgradeV1V2DarPath,
    coinUpgradeV1V3DarPath,
  )

  "softFetch" should {
    "succeed when given a contract id of the same type Coin V1" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v1_softFetch_v1"),
            dar = coinUpgradeV1V2Dar,
          )
      } yield r shouldBe SUnit
    }

    "succeed when given a contract id of the same type Coin V2" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v2_softFetch_v2"),
            dar = coinUpgradeV1V2Dar,
          )
      } yield r shouldBe SUnit
    }

    "succeed when given a contract id of a predecessor type of Coin V2, Coin V1" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v1_softFetch_v2"),
            dar = coinUpgradeV1V2Dar,
          )
      } yield r shouldBe SUnit
    }

    // TODO: https://github.com/digital-asset/daml/issues/16151
    // reenable test once `WronglyTypedContractSoft` is handled in Canton
    "fail when given a contract id of a non-predecessor type of Coin V1, Coin V2" ignore {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v2_softFetch_v1"),
            dar = coinUpgradeV1V2Dar,
          )
      } yield r shouldBe SUnit
    }

    "succeed when given a contract id of a transitive predecessor type of Coin V3, Coin V1" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v1_softFetch_v3"),
            dar = coinUpgradeV1V3Dar,
          )
      } yield r shouldBe SUnit
    }

    // TODO: https://github.com/digital-asset/daml/issues/16151
    // reenable test once `WronglyTypedContractSoft` is handled in Canton
    "fail when given a contract id of a non-predecessor type of Coin V1, Coin V3" ignore {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v3_softFetch_v1"),
            dar = coinUpgradeV1V3Dar,
          )
      } yield r shouldBe SUnit
    }
  }
}
