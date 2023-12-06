// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

sealed trait FeatureFlag {
  def configName: String
}

object FeatureFlag {

  object Stable extends FeatureFlag {
    val configName = "enabled-by-default"
    override def toString: String = "Stable"
  }

  object Preview extends FeatureFlag {
    val configName = "enable-preview-commands"
    override def toString: String = "Preview"
  }

  object Repair extends FeatureFlag {
    val configName = "enable-repair-commands"
    override def toString: String = "Repair"
  }

  object Testing extends FeatureFlag {
    val configName = "enable-testing-commands"
    override def toString: String = "Testing"
  }

  lazy val all = Set(Stable, Preview, Repair, Testing)

}
