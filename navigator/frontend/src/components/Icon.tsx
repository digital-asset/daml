// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  IconName as ComponentIconName,
  IconType,
  UntypedIcon,
} from '@da/ui-core';

export type IconName
  = ComponentIconName
  | 'sign-out'
  | 'clock'
  | 'user';

export const Icon = UntypedIcon as IconType<IconName>;
