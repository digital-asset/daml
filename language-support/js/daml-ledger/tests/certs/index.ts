// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'fs';

export const rootCertChain: Buffer = readFileSync(`${__dirname}/ca.crt`);
export const serverCertChain: Buffer = readFileSync(`${__dirname}/server.crt`);
export const serverPrivateKey: Buffer = readFileSync(`${__dirname}/server.key`);
export const clientCertChain: Buffer = readFileSync(`${__dirname}/client.crt`);
export const clientPrivateKey: Buffer = readFileSync(`${__dirname}/client.key`);