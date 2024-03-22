// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import Strong from "../Strong";

export default (): JSX.Element => (
  <Section
    title="Show text in bold"
    description="This component shows text with **strong** emphasis.">
    <Strong>Strong text example</Strong>
  </Section>
);
