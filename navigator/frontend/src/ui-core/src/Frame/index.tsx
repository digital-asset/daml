// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import styled, { hardcodedStyle } from "../theme";

export interface FrameProps {
  top: React.ReactNode;
  left: React.ReactNode;
  content: React.ReactNode;
}

const Wrapper = styled.div`
  height: 100%;
  padding-right: 4rem;
  padding-left: 2rem;
  padding-top: 2rem;
  padding-bottom: 4rem;
  display: flex;
  flex-direction: column;
  background: ${({ theme }) => theme.documentBackground};
`;

const Bar = styled.div`
  overflow: hidden;
  padding-bottom: 2rem;
`;

const Main = styled.div`
  display: flex;
  align-content: center;
  flex: 1;
`;

const Side = styled.div`
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 1;
  padding-right: 1.5rem;
  padding-top: 1rem;
  width: ${hardcodedStyle.sidebarWidth};
  min-width: ${hardcodedStyle.sidebarWidth};
`;

const Content = styled.div`
  color: ${({ theme }) => theme.colorForeground};
  background-color: ${({ theme }) => theme.colorBackground};
  display: flex;
  flex-grow: 0;
  flex-shrink: 1;
  height: 100%;
  width: 100%;
  overflow: auto;
  position: relative;
`;

/**
 * The <Frame> component renders a default page frame, consisting of:
 * - A full-width navigation bar at the top
 * - A side bar box, left of the content box
 * - Content box, right of the sidebar box
 */
const Frame: React.FC<FrameProps> = ({ top, left, content }) => (
  <Wrapper>
    <Bar>{top}</Bar>
    <Main>
      <Side>{left}</Side>
      <Content>{content}</Content>
    </Main>
  </Wrapper>
);

export default Frame;
