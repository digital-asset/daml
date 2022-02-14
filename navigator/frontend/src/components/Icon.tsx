// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  IconName as ComponentIconName,
  IconType,
  UntypedIcon,
} from "@da/ui-core";

export type IconName = ComponentIconName | "sign-out" | "clock" | "user";

export const Icon = UntypedIcon as IconType<IconName>;
