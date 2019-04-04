// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.ledger.client.binding.encoding.GenEncoding
import com.digitalasset.slick.PostgreSqlConnectionWithContainer
import org.scalatest.Ignore

@Ignore // https://digitalasset.atlassian.net/browse/DEL-5521
class SlickIntegrationPostgreSqlSpec
    extends GenericMultiTableScenario(new PostgreSqlConnectionWithContainer()) {
  override def genDerivation = GenEncoding.postgresSafe
}
