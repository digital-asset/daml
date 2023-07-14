// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.speedy.SValue._
import java.nio.file.Paths
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class DevIT extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {
  final override protected lazy val devMode = true
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  lazy val devDarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/script-test-1.dev.dar"))
  lazy val devDar = CompiledDar.read(devDarPath, Runner.compilerConfig)

  lazy val coinV1DarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-v1.dar"))
  lazy val coinV2DarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-v2.dar"))
  lazy val coinV2NewFieldDarPath =
    BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-v2-new-field.dar"))
  lazy val coinV3DarPath = BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-v3.dar"))
  lazy val coinUpgradeV1V2DarPath =
    BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-upgrade-v1-v2.dar"))
  lazy val coinUpgradeV1V2NewFieldDarPath =
    BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-upgrade-v1-v2-new-field.dar"))
  lazy val coinUpgradeV1V3DarPath =
    BazelRunfiles.rlocation(Paths.get("daml-script/test/coin-upgrade-v1-v3.dar"))
  lazy val coinUpgradeV1V2Dar: CompiledDar =
    CompiledDar.read(coinUpgradeV1V2DarPath, Runner.compilerConfig)
  lazy val coinUpgradeV1V2NewFieldDar: CompiledDar =
    CompiledDar.read(coinUpgradeV1V2NewFieldDarPath, Runner.compilerConfig)
  lazy val coinUpgradeV1V3Dar: CompiledDar =
    CompiledDar.read(coinUpgradeV1V3DarPath, Runner.compilerConfig)

  override protected lazy val darFiles = List(
    devDarPath,
    coinV1DarPath,
    coinV2DarPath,
    coinV2NewFieldDarPath,
    coinV3DarPath,
    coinUpgradeV1V2DarPath,
    coinUpgradeV1V2NewFieldDarPath,
    coinUpgradeV1V3DarPath,
  )

  // TODO: https://github.com/digital-asset/daml/issues/15882
  // -- Enable this test when canton supports choice observers
  "ChoiceAuthority:test" should {
    "succeed" ignore {
      for {
        clients <- scriptClients()
        v <- run(
          clients,
          QualifiedName.assertFromString("TestChoiceAuthority:test"),
          dar = devDar,
        )
      } yield {
        v shouldBe (SUnit)
      }
    }
  }

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

    "succeed when given a contract id of a predecessor type of Coin V2 (with a new field), Coin V1 -- Upgrade" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v1_softFetch_v2"),
            dar = coinUpgradeV1V2NewFieldDar,
          )
      } yield r shouldBe SUnit
    }

    "succeed when given a contract id of a non-predecessor type of Coin V1, Coin V2 -- Downgrade" in {
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

    "succeed when given a contract id of a non-predecessor type of Coin V1, Coin V2 (with new field = None) -- Downgrade/drop-None" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v2_none_softFetch_v1"),
            dar = coinUpgradeV1V2NewFieldDar,
          )
      } yield r shouldBe SUnit
    }

    "fail when given a contract id of a non-predecessor type of Coin V1, Coin V2 (with new field = Some _) -- refuse Downgrade/drop-Some" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v2_some_softFetch_v1"),
            dar = coinUpgradeV1V2NewFieldDar,
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

    "succeed when given a contract id of a non-predecessor type of Coin V1, Coin V3 -- Downgrade" in {
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

  "softExercise" should {
    "succeed when given a contract id of the same type Coin V2" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("CoinUpgrade:create_v2_softExercise_v2"),
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
            QualifiedName.assertFromString("CoinUpgrade:create_v1_softExercise_v2"),
            dar = coinUpgradeV1V2Dar,
          )
      } yield r shouldBe SUnit
    }
  }

}
