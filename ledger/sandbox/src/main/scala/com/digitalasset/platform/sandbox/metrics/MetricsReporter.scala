// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.metrics

import java.nio.file.Path

sealed trait MetricsReporter

object MetricsReporter {

  case object Console extends MetricsReporter

  final case class Csv(directory: Path) extends MetricsReporter

}
