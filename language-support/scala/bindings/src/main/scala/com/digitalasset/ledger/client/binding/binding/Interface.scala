// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

/** Common superclass of interface marker types.  There are no instances of
  * subclasses of this class; it is strictly a marker type to aid in implicit
  * resolution, and only occurs within contract IDs.
  */
abstract class Interface extends VoidValueRef

object Interface {
  // TODO #13924 unsafeToTemplate extension method for cids
}
