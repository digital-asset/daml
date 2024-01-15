// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import Link from "../Link";

const description = `Single-page application links handles modifier keys and
prevents default in favour of an \`onClick\` handler.`;

// Define type for exported IconGuide component such that clients can specialise
// it.
export default (): JSX.Element => (
  <Section title="Single-page application links" description={description}>
    <Link
      href="/click-me"
      onClick={() => {
        alert("Clicked link!");
      }}>
      Click me!
    </Link>
  </Section>
);
