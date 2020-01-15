// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.crypto

import scala.collection.concurrent

/**
  * The purpose of a hash serves to avoid hash collisions due to equal encodings for different objects.
  * It is in general not possible to derive the purpose of the hash from the hash alone.
  *
  * Whenever a hash is computed using [[SHA256Hash.Builder]], a [[HashPurpose]] must be specified that gets included in the hash.
  * To reliably prevent hash collisions,
  * 1 - A new, globally unique hash purpose must be defined for each type T for whose values we want to compute hashes for
  * 2 - If T is a sum type, it must define, for each variant V, a tag that is unique to V in the scope of T (i.e.,
  *    variant tags may be reused across Ts), such that ...
  *
  * All [[HashPurpose]] objects must be created through the [[HashPurpose$.apply]] method, which checks that the id is
  * fresh.
  *
  * @param id The identifier for the [[HashPurpose]].
  *           Every [[HashPurpose]] object must have a unique [[id]].
  */
class HashPurpose private (val id: Int, val description: String) extends AnyVal

object HashPurpose {

  private val values: concurrent.Map[Int, HashPurpose] = concurrent.TrieMap.empty[Int, HashPurpose]

  /** Creates a new [[HashPurpose]] with a given description */
  def apply(id: Int, description: String): HashPurpose = {
    val purpose = new HashPurpose(id, description)
    values
      .putIfAbsent(id, purpose)
      .foreach { oldPurpose =>
        throw new IllegalArgumentException(
          s"HashPurpose with id=$id already exists for ${oldPurpose.description}")
      }
    purpose
  }

  /* HashPurposes are listed as `val` rather than `case object`s such that they are initialized eagerly.
   * This ensures that HashPurpose id clashes are detected eagerly. Otherwise, it may be there are two hash purposes
   * with the same id, but they are never used in the same Java process and therefore the clash is not detected.
   */
  val Testing = HashPurpose(0, "Testing")
  val ContractKey = HashPurpose(1, "ContractKey")
}
