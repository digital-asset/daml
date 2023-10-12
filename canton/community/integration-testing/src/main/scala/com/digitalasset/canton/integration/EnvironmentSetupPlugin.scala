// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.NamedLogging

/** Plugin to allow multiple tests to reuse behavior modifying the test config or environment without having to modify
  * the test definition.
  * For instance running tests against Postgres vs the default config, with the plugin being able to provide or verify
  * the backend database exists.
  * Will work regardless of whether the test is using [[IsolatedEnvironments]] or [[SharedEnvironment]] integration
  * test setups.
  * Must call [[EnvironmentSetup.registerPlugin]] within its constructor to register the plugin.
  */
trait EnvironmentSetupPlugin[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends NamedLogging {

  /** Run before any of the tests in the test class have been run or an environment created */
  def beforeTests(): Unit = {}

  /** Run after all of the tests in the test class have finished and the environment has been torn down */
  def afterTests(): Unit = {}

  /** Hook before the environment has been created.
    * Returned config will be used for the test.
    */
  def beforeEnvironmentCreated(config: E#Config): E#Config = config

  /** Hook after the environment has been created but no tests have yet been run */
  def afterEnvironmentCreated(config: E#Config, environment: TCE): Unit = {}

  /** Hook after all tests from the test class have completed but the environment is still running */
  def beforeEnvironmentDestroyed(config: E#Config, environment: TCE): Unit = {}

  /** Hook after the tests have been run and the environment has been shutdown */
  def afterEnvironmentDestroyed(config: E#Config): Unit = {}
}
