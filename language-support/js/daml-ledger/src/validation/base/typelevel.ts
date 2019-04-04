// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Validation } from ".";

/**
 * Extracts required keys as a literal type union
 */
export type RequiredKeys<T> = { [K in keyof T]: {} extends Pick<T, K> ? never : K } extends { [_ in keyof T]: infer U }
  ? {} extends U
    ? never
    : U
  : never

/**
 * Extracts optional keys as a literal type union
 */
export type OptionalKeys<T> = { [K in keyof T]: T extends Record<K, T[K]> ? never : K } extends {
  [_ in keyof T]: infer U
}
  ? {} extends U
    ? never
    : U
  : never

export type RequiredSlice<K extends object, V> = Record<RequiredKeys<K>, V>
export type RequiredValidation<K extends object> = RequiredSlice<K, Validation>

export type OptionalSlice<K extends object, V> = Record<OptionalKeys<K>, V>
export type OptionalValidation<K extends object> = OptionalSlice<K, Validation>