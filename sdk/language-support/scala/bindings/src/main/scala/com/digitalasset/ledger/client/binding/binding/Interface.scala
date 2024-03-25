// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

    /** Convert an interface contract ID to a template contract ID.  Sometimes
      * this is needed if you got an interface contract ID from a choice, but
      * you need to assert that the contract ID is of a particular template
      * so that you can exercise contracts on it.
      *
      * This checks at compile-time that `T` is in fact a template that
      * implements interface `I`, but it does not check that the specific
      * contract ID is actually associated with `T` on the ledger, hence the
      * `unsafe` in the name.
      */
    @nowarn("cat=unused&msg=parameter ev in method")
    def unsafeToTemplate[T](implicit ev: Template.Implements[T, I]): ContractId[T] = {
      type K[C] = C => ApiTypes.ContractId
      type K2[C] = ContractId[I] => C
      subst[K2, T](subst[K, I](identity))(self)
    }
  }
}
