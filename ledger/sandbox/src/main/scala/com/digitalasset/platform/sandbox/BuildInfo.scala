// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.{BufferedReader, InputStreamReader}

object BuildInfo {
  val Version = {
    Option(this.getClass.getClassLoader.getResourceAsStream("COMPONENT-VERSION")).fold {
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
}
