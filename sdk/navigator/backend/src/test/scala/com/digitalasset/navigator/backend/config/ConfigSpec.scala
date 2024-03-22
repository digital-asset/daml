// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.config

import com.daml.ledger.api.refinements.ApiTypes.Party
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec

class RowSpec extends AnyFreeSpec with Matchers {
  private val defaultConfigText = "users { Alice { party = alice }, Bob { party = bob } }"
  private val defaultConfig = Config(
    Map(
      "Alice" -> UserConfig(Party("alice"), None, false),
      "Bob" -> UserConfig(Party("bob"), None, false),
    )
  )

  private def withConfig[A](f: Path => A): A = {
    val path = Files.createTempFile("navigator", ".conf")
    try {
      Files.write(path, defaultConfigText.getBytes(StandardCharsets.UTF_8))
      f(path)
    } finally {
      Files.delete(path)
    }
  }
  "Config" - {
    "load" - {
      "loads explicit config" in {
        withConfig { conf =>
          Config.load(ExplicitConfig(conf), false) shouldBe Right(defaultConfig)
        }
      }
      "fails if explicit config does not exist" in {
        Config.load(ExplicitConfig(Paths.get("nonexistentgarbage")), false) shouldBe Left(
          ConfigNotFound("File nonexistentgarbage not found")
        )
      }
      "loads default config if none is specified" in {
        withConfig { conf =>
          Config.load(DefaultConfig(conf), false) shouldBe Right(defaultConfig)
        }
      }
      "loads an empty config if default config does not exist" in {
        Config.load(DefaultConfig(Paths.get("nonexistentgarbage")), false) shouldBe Right(Config())
      }
    }
  }
}
