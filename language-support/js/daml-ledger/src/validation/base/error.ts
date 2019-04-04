// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export type Error = MissingKey | TypeError | UnexpectedKey | NonUniqueUnion

export interface MissingKey {
    kind: 'missing-key'
    expectedKey: string
    expectedType: string
}

export interface TypeError {
    kind: 'type-error'
    expectedType: string
    actualType: string
}

export interface UnexpectedKey {
    kind: 'unexpected-key'
    key: string
}

export interface NonUniqueUnion {
    kind: 'non-unique-union',
    keys: string[]
}