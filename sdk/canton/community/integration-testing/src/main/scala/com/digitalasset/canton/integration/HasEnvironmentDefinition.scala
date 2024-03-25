// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.environment.Environment

/** Trait for exposing only an environment definition */
trait HasEnvironmentDefinition[E <: Environment, TCE <: TestConsoleEnvironment[E]] {
  def environmentDefinition: BaseEnvironmentDefinition[E, TCE]
}
