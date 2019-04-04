// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen
import com.digitalasset.slick.H2Db

class SlickIntegrationH2Spec extends GenericMultiTableScenario(new H2Db())
