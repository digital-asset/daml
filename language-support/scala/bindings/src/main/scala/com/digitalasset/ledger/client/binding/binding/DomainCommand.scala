// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding
import com.digitalasset.ledger.api.v1.commands.Command

case class DomainCommand(command: Command, template: TemplateCompanion[_])
