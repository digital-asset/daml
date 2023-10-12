// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.traverse.*

object VersionUtil {
  def create(
      rawVersion: String,
      baseName: String,
  ): Either[String, (Int, Int, Int, Option[String])] = {
    // `?:` removes the capturing group, so we get a cleaner pattern-match statement
    val regex = raw"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,4})(?:-(.*))?".r

    rawVersion match {
      case regex(rawMajor, rawMinor, rawPatch, suffix) =>
        val parsedDigits = List(rawMajor, rawMinor, rawPatch).traverse(raw =>
          raw.toIntOption.toRight(s"Couldn't parse number `$raw`")
        )
        parsedDigits.flatMap {
          case List(major, minor, patch) =>
            // `suffix` is `null` if no suffix is given
            Right((major, minor, patch, Option(suffix)))
          case _ => Left(s"Unexpected error while parsing version `$rawVersion`")
        }

      case _ =>
        Left(
          s"Unable to convert string `$rawVersion` to a valid ${baseName}. A ${baseName} is similar to a semantic version. For example, '1.2.3' or '1.2.3-SNAPSHOT' are valid ${baseName}s."
        )
    }
  }
}
