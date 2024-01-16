// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import pureconfig.ConfigWriter
import pureconfig.generic.DerivedConfigWriter
import pureconfig.generic.semiauto.deriveWriter
import shapeless.Lazy

class ConfidentialConfigWriter(confidential: Boolean) {
  def apply[Config](
      map: Config => Config
  )(implicit writer: Lazy[DerivedConfigWriter[Config]]): ConfigWriter[Config] = {
    val parent = deriveWriter[Config]
    if (confidential)
      (a: Config) => parent.to(map(a))
    else
      parent
  }
}
