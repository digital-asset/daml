// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.metrics.MetricName

package object execution {

  private[execution] val MetricPrefix = MetricName.DAML :+ "commands" :+ "execution"

}
