// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

/** Thrown when a persisted value cannot be deserialized back into a scala type. */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class DbDeserializationException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** Thrown when a value is persisted into the database. */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class DbSerializationException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** Thrown when the db has not been properly initialized */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class DbUninitializedException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
