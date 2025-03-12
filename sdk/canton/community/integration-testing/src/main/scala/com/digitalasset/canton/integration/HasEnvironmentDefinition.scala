// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

/** Trait for exposing only an environment definition */
trait HasEnvironmentDefinition[TCE <: TestConsoleEnvironment] {
  def environmentDefinition: BaseEnvironmentDefinition[TCE]
}
