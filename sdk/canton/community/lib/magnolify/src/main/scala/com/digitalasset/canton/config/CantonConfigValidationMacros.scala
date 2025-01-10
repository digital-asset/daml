// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

import scala.reflect.macros.whitebox

object CantonConfigValidationMacros {
  def genCantonConfigValidatorMacro[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe.*
    val wtt = weakTypeTag[T]
    q"""_root_.com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation.apply[$wtt]"""
  }
}
