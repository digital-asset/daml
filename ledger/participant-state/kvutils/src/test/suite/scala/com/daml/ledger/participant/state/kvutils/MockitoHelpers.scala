// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import org.mockito.ArgumentCaptor

import scala.reflect.ClassTag

object MockitoHelpers {

  def captor[T](implicit classTag: ClassTag[T]): ArgumentCaptor[T] =
    ArgumentCaptor.forClass(classTag.runtimeClass.asInstanceOf[Class[T]])

}
