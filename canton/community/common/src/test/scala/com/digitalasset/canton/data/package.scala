// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

package object data {
  implicit class RichFullInformeeTree(val v: FullInformeeTree) extends AnyVal {
    def toInformeeTree = InformeeTree.tryCreate(v.tree, BaseTest.testedProtocolVersion)
  }

  implicit class RichInformeeTree(val v: InformeeTree) extends AnyVal {

    /** @throws InformeeTree$.InvalidInformeeTree if this is not a full informee tree (i.e. the wrong nodes are blinded)
      */
    def tryToFullInformeeTree = FullInformeeTree.tryCreate(v.tree, BaseTest.testedProtocolVersion)
  }
}
