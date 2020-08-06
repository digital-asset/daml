// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.resources

import com.daml.resources.ResourceOwnerFactories
import com.daml.resources.akka.AkkaResourceOwnerFactories

object ResourceOwner extends ResourceOwnerFactories with AkkaResourceOwnerFactories
