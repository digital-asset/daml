// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

 /**
  * A Mapping between a Protocol Buffers message M and a plain object specified by an interface O
  */
export interface Mapping<M, O> {

    /**
     * Converts a plain JS object to a Protocol Buffers message following this mapping
     * @param object A plain JS object
     */
    toMessage(object: O): M

    /**
     * Converts a Protocol Buffers message to a plain JS object following this mapping
     */
    toObject(message: M): O

}
