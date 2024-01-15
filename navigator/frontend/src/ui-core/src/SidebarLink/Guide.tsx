// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import { default as Link } from "../Link";
import styled, { hardcodedStyle } from "../theme";
import { makeSidebarLink } from "./index";

const SidebarItem = makeSidebarLink(Link);

function logClick() {
  console.log("CLICK!");
}

const Wrapper = styled.div`
  max-width: ${hardcodedStyle.sidebarWidth};
  padding: 0.5em;
`;

const OuterWrapper = styled.div`
  background: ${({ theme }) => theme.documentBackground};
`;

export default (): JSX.Element => (
  <Section
    title="Sidebar navigation links"
    description="Link styles for the navigation sidebar">
    <OuterWrapper>
      <Wrapper>
        <SidebarItem
          onClick={logClick}
          icon="chevron-right"
          title="Link with icon"
          href="#"
        />
        <SidebarItem
          onClick={logClick}
          title="Link with a rather long title"
          href="#"
          count={1}
        />
        <SidebarItem
          onClick={logClick}
          title="Active option"
          href="#"
          isActive={true}
          count={1204}
        />
      </Wrapper>
    </OuterWrapper>
  </Section>
);
