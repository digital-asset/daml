// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{ErrorClass, ErrorGroup}

object EnterpriseSequencerErrorGroups {

  private implicit val errorClass: ErrorClass = ErrorClass.root()

  abstract class EthereumErrorGroup extends ErrorGroup

  abstract class FabricErrorGroup extends ErrorGroup
}
