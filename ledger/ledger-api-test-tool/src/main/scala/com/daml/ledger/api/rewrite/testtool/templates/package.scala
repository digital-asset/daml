// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool

import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.apitesting.TestTemplateIds

package object templates {

  val ids = new TestTemplateIds(PlatformApplications.Config.default).templateIds

}
