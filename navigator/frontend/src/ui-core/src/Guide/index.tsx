// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import ReactMarkdown from "react-markdown";
import styled from "../theme";

/**
 * This module contains components for the component guide. A guide is a 'Guide'
 * component with 'Section' components in it.
 */

const showcaseProportion = 0.6;

const Container = styled.div`
  max-width: ${({ theme }) => theme.guideWidthMax};
  min-width: ${({ theme }) => theme.guideWidthMin};
  margin: 0 auto;
  padding: 0 0.5rem;
`;

const Header = styled.header`
  background-color: ${({ theme }) => theme.colorPrimary[0]};
  color: ${({ theme }) => theme.colorPrimary[1]};
  padding-top: 1rem;
  padding-bottom: 1rem;
`;

const Logo = styled.div`
  height: 3rem;
  img {
    height: 100%;
  }
`;

const Title = styled.h1`
  font-weight: 400;
  font-size: 3rem;
`;

const GuideDescription = styled(ReactMarkdown)`
  font-weight: 400;
`;

const SectionWrapper = styled.section`
  background-color: ${({ theme }) => theme.colorBackground};
  color: ${({ theme }) => theme.colorForeground};
  padding-top: 0.5rem;
  padding-bottom: 0.5rem;
`;

const SectionTitle = styled.h3`
  font-weight: 700;
  font-size: 1.25rem;
  margin-top: 1rem;
  margin-bottom: 1rem;
  color: ${({ theme }) => theme.colorForeground};
  border-bottom: 1px solid transparent;
  border-bottom-color: ${({ theme }) => theme.colorForeground};
`;

const SectionSplit = styled.div`
  display: flex;
  width: 100%;
`;

const SectionShowcase = styled.div`
  flex: ${showcaseProportion};
  width: 100%;
`;

const SectionDescription = styled(ReactMarkdown)`
  flex: calc(1 - ${showcaseProportion});
  padding-left: 1rem;
`;

export interface GuideProps {
  title: string;
  logo?: string;
  description?: string;
  className?: string;
}

export const Guide: React.FC<GuideProps> = props => (
  <main className={props.className}>
    <Header>
      <Container>
        {props.logo ? (
          <Logo>
            <img src={props.logo} />
          </Logo>
        ) : null}
        <Title>{props.title}</Title>
        {props.description ? (
          <GuideDescription source={props.description} />
        ) : null}
      </Container>
    </Header>
    {props.children}
  </main>
);

export interface SectionProps {
  title: string;
  description?: string;
  className?: string;
}

export const Section: React.StatelessComponent<SectionProps> = props => (
  <SectionWrapper className={props.className}>
    <Container>
      <SectionTitle>{props.title}</SectionTitle>
      <SectionSplit>
        <SectionShowcase>{props.children}</SectionShowcase>
        {props.description ? (
          <SectionDescription source={props.description} />
        ) : null}
      </SectionSplit>
    </Container>
  </SectionWrapper>
);
