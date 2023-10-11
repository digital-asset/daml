// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import pureconfig.ConfigReader
import pureconfig.generic.FieldCoproductHint

object CantonConfigUtil {

  /** By default pureconfig will expect our H2 config to use the value `h-2` for type.
    * This just changes this expectation to being lower cased so `h2` will work.
    */
  def lowerCaseStorageConfigType[SC <: StorageConfig]: FieldCoproductHint[SC] =
    new FieldCoproductHint[SC]("type") {
      // Keep the following case classes of ADTs as lowercase and not kebab-case
      override def fieldValue(name: String): String = name.toLowerCase
    }

  implicit class ExtendedConfigReader[T](val configReader: ConfigReader[T]) extends AnyVal {
    def enableNestedOpt(
        enableField: String = "enabled",
        disableNestedField: T => T,
    ): ConfigReader[T] = { cursor =>
      val result = for {
        objCur <- cursor.asObjectCursor
        autoInitCur <- objCur.atKey(enableField)
        autoInit <- autoInitCur.asBoolean
        withoutAutoInitKey = objCur.withoutKey(enableField)
        identity <-
          if (!autoInit) configReader.from(withoutAutoInitKey).map(disableNestedField)
          else configReader.from(withoutAutoInitKey)
      } yield identity

      result.orElse(configReader.from(cursor))
    }
  }

}
