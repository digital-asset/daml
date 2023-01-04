// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.buildinfo

import java.io.{BufferedReader, InputStreamReader}

import scala.jdk.CollectionConverters._

object BuildInfo {
  val Version: String =
    Option(getClass.getClassLoader.getResourceAsStream("MVN_VERSION")).fold {
      "{component version not found on classpath}"
    } { is =>
      try {
        val reader = new BufferedReader(new InputStreamReader(is))
        reader.lines.iterator.asScala.mkString.trim
      } finally {
        is.close()
      }
    }
}
