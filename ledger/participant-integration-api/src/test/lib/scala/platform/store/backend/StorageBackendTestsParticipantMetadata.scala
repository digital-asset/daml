// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.nio.charset.StandardCharsets
import java.sql.SQLException

import com.daml.platform.store.backend.localstore.ParticipantMetadataBackend
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

trait TestedResource {
  def createResourceAndReturnInternalId(): Int
  def fetchResourceVersion(): Long
}

private[backend] trait ParticipantResourceMetadataTests
    extends Matchers
    with StorageBackendSpec
    with OptionValues {
  this: AnyFlatSpec =>

  private def tested = ParticipantMetadataBackend

  def resourceVersionTableName: String
  def resourceAnnotationsTableName: String
  def newResource(): TestedResource

  it should "compare and swap resource version" in {
    val resource = newResource()
    val internalId = resource.createResourceAndReturnInternalId()
    resource.fetchResourceVersion() shouldBe 0
    executeSql(
      tested.compareAndIncreaseResourceVersion(resourceVersionTableName)(
        internalId = internalId,
        expectedResourceVersion = 0,
      )
    ) shouldBe true
    executeSql(
      tested.compareAndIncreaseResourceVersion(resourceVersionTableName)(
        internalId = internalId,
        expectedResourceVersion = 404,
      )
    ) shouldBe false
    executeSql(
      tested.compareAndIncreaseResourceVersion(resourceVersionTableName)(
        internalId = internalId,
        expectedResourceVersion = 1,
      )
    ) shouldBe true
    resource.fetchResourceVersion() shouldBe 2
  }

  it should "get, add and delete user's annotations" in {
    val resource = newResource()
    val internalId = resource.createResourceAndReturnInternalId()
    executeSql(tested.getAnnotations(resourceAnnotationsTableName)(internalId)) shouldBe Map.empty
    // Add key1
    executeSql(
      tested.addAnnotation(resourceAnnotationsTableName)(
        internalId,
        key = "key1",
        value = "value1",
        updatedAt = 123,
      )
    )
    executeSql(tested.getAnnotations(resourceAnnotationsTableName)(internalId)) shouldBe Map(
      "key1" -> "value1"
    )
    // Add key2
    executeSql(
      tested.addAnnotation(resourceAnnotationsTableName)(
        internalId,
        key = "key2",
        value = "value2",
        updatedAt = 123,
      )
    )
    executeSql(tested.getAnnotations(resourceAnnotationsTableName)(internalId)) shouldBe Map(
      "key1" -> "value1",
      "key2" -> "value2",
    )
    // Duplicated key2
    assertThrows[SQLException](
      executeSql(
        tested
          .addAnnotation(resourceAnnotationsTableName)(
            internalId,
            key = "key2",
            value = "value2b",
            updatedAt = 123,
          )
      )
    )
    // Delete
    executeSql(tested.deleteAnnotations(resourceAnnotationsTableName)(internalId))
    executeSql(tested.getAnnotations(resourceAnnotationsTableName)(internalId)) shouldBe Map.empty
  }

  it should "store and retrieve an empty annotation value" in {
    val resource = newResource()
    val internalId = resource.createResourceAndReturnInternalId()
    executeSql(
      tested.addAnnotation(resourceAnnotationsTableName)(
        internalId,
        key = "key",
        value = "",
        updatedAt = 0,
      )
    )
    executeSql(tested.getAnnotations(resourceAnnotationsTableName)(internalId)) shouldBe Map(
      "key" -> ""
    )
  }

  it should "allow to store 256kb of annotations (counted in utf-8 bytes)" in {
    val resource = newResource()
    val internalId = resource.createResourceAndReturnInternalId()
    val key = "key"
    val value = "a" * (256 * 1024 - 3)
    (key.getBytes(StandardCharsets.UTF_8).length + value
      .getBytes(StandardCharsets.UTF_8)
      .length) shouldBe 256 * 1024
    executeSql(
      tested.addAnnotation(resourceAnnotationsTableName)(
        internalId,
        key = key,
        value = value,
        updatedAt = 0,
      )
    )
  }

  it should "allow to store key of length 317 " in {
    val resource = newResource()
    val internalId = resource.createResourceAndReturnInternalId()
    val longestKeyPrefix = "a" * 253
    val longestKeyName = "b" * 63
    val longestKey = s"$longestKeyPrefix/$longestKeyName"
    longestKey should have length (317)
    executeSql(
      tested.addAnnotation(resourceAnnotationsTableName)(
        internalId,
        key = longestKey,
        value = "longest key",
        updatedAt = 0,
      )
    )
  }

  it should "store and retrieve trailing spaces in annotation keys" in {
    val resource = newResource()
    val internalId = resource.createResourceAndReturnInternalId()
    val key = "key"
    val value = "a" + " " * 10
    executeSql(
      tested.addAnnotation(resourceAnnotationsTableName)(
        internalId,
        key = key,
        value = value,
        updatedAt = 0,
      )
    )
    executeSql(tested.getAnnotations(resourceAnnotationsTableName)(internalId)) shouldBe
      Map(key -> value)
  }

}
