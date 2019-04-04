// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import org.scalatest.{WordSpec, Matchers}

class LanguageMajorVersionSpec extends WordSpec with Matchers {
  "DamlLfVersionDev" should {
    import LanguageMajorVersion.VDev
    "exist" in {
      VDev
    }

    "have a SHA-256-sized version" in {
      val hash = VDev.maxSupportedMinorVersion
      hash should have length 64
      hash should fullyMatch regex "[0-9a-f]+"
    }
  }
}
