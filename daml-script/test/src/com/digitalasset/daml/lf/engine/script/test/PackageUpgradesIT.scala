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
  val coinUpgradeDarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-upgrade.dar"))
  val coinV1Dar: CompiledDar = readDar(coinV1DarPath, Runner.compilerConfig)
  val coinV2Dar: CompiledDar = readDar(coinV2DarPath, Runner.compilerConfig)
  val coinUpgradeDar: CompiledDar = readDar(coinUpgradeDarPath, Runner.compilerConfig)

  override protected lazy val darFiles = List(
    coinV1DarPath,
    coinV2DarPath,
    coinUpgradeDarPath,
  )

  "softFetch" should {
    "succeed when given a contract id of the same type Coin V1" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v1_softFetch_v1"),
            dar = coinUpgradeDar,
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
            dar = coinUpgradeDar,
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
            dar = coinUpgradeDar,
          )
      } yield r shouldBe SUnit
    }

    "fail when given a contract id of a non-predecessor type of Coin V1, Coin V2" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v2_softFetch_v1"),
            dar = coinUpgradeDar,
          )
      } yield r shouldBe SUnit
    }
  }
}
