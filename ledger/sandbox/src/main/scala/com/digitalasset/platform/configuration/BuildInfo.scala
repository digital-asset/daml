// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.configuration

import java.io.{BufferedReader, InputStreamReader}

object BuildInfo {
  val Version: String =
    Option(this.getClass.getClassLoader.getResourceAsStream("MVN_VERSION")).fold {
      "{component version not found on classpath}"
    } { is =>
      try {
        val reader = new BufferedReader(new InputStreamReader(is))
        reader.lines().reduce("", (t: String, u: String) => t + u).trim

      } finally {
        is.close()
      }
    }
}
