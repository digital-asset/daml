// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import Breadcrumbs from '../Breadcrumbs';
import Button from '../Button';
import { Section } from '../Guide';

const description = `The \`Breadcrumbs\` component accepts any type of elements
as children and will separate each by a divider.
`;

export default () => (
  <Section
    title="Example breadcrumbs"
    description={description}
  >
    <div>
      <Breadcrumbs>
        <a href="#">Home</a>
        <a href="#">Guide</a>
      </Breadcrumbs>
      <Breadcrumbs>
        You can
        <Button onClick={() => { ; }} type="main">
          put
        </Button>
        <em>anything</em>
        in the Breadcrumbs component
      </Breadcrumbs>
    </div>
  </Section>
);
