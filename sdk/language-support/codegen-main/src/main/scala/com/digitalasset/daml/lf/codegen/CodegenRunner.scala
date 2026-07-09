// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import com.daml.assistant.config.{ConfigLoadingError, PackageConfig}
import scopt.OptionParser

trait CodegenRunner {
  type Config

  def configParser(isDpm: Boolean): OptionParser[Config]
  def configureFromArgs(args: Array[String], isDpm: Boolean): Option[Config]
  def configureFromPackageConfig(packageConfig: PackageConfig): Either[ConfigLoadingError, Config]
  def generateCode(config: Config, damlVersion: String): Unit
}
