// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { decode } from 'jwt-simple';

const LEDGER_ID: string = 'DAVL';

export type Credentials = {
  party: string;
  token: string;
}

/**
 * Check that the party in the token matches the party of the credentials and
 * that the ledger ID in the token matches `LEDGER_ID`.
 */
export const preCheckCredentials = ({party, token}: Credentials): string | null => {
  const decoded = decode(token, '', true);
  if (!decoded.ledgerId || decoded.ledgerId !== LEDGER_ID) {
    return 'The password is not valid for DAVL.';
  }
  if (!decoded.party || decoded.party !== party) {
    return 'The password is not valid for this user.';
  }
  return null;
}

export default Credentials;
