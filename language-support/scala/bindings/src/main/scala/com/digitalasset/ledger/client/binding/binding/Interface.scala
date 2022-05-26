// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.refinements.ApiTypes

import scala.annotation.nowarn

/** Common superclass of interface marker types.  There are no instances of
  * subclasses of this class; it is strictly a marker type to aid in implicit
  * resolution, and only occurs within contract IDs.
  */
abstract class Interface extends VoidValueRef

object Interface {
  import Primitive.ContractId, ContractId.subst

  implicit final class `interface ContractId syntax`[I](private val self: ContractId[I])
      extends AnyVal {
    @nowarn("cat=unused&msg=parameter value ev in method")
    def unsafeToTemplate[T](implicit ev: Template.Implements[T, I]): ContractId[T] = {
      type K[C] = C => ApiTypes.ContractId
      type K2[C] = ContractId[I] => C
      subst[K2, T](subst[K, I](identity))(self)
    }
  }
}
