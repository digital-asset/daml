// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

package object engine {

  @deprecated("use ValuePostProcessor", since = "3.2.0")
  type ValueEnricher = ValuePostprocessor

}
