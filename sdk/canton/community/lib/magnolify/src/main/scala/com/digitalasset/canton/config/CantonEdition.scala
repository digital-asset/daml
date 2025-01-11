// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

sealed trait CantonEdition extends Product with Serializable
case object CommunityCantonEdition extends CantonEdition
case object EnterpriseCantonEdition extends CantonEdition
