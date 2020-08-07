// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import com.daml.resources.AbstractResourceOwner

package object resources {

  type ResourceOwner[+A] = AbstractResourceOwner[ResourceContext, A]

}
