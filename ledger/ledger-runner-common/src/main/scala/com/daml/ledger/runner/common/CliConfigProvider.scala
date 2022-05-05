// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import scopt.OptionParser

trait CliConfigProvider[ExtraConfig] {

  def defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[CliConfig[ExtraConfig]]): Unit

}
