// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { User } from "@daml/ledger";

export type Credentials = {
  party: string;
  publicParty: string;
  token: string;
  user: User;
};

export default Credentials;
