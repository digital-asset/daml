// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import "./styles.css";

import * as React from "react";
import { ApolloClient, ApolloProvider } from "@apollo/client";
import * as ReactDOM from "react-dom";
import ArgumentDisplayGuide from "../ArgumentDisplay/Guide";
import AutosuggestGuide from "../Autosuggest/Guide";
import BreadcrumbsGuide from "../Breadcrumbs/Guide";
import ButtonGuide from "../Button/Guide";
import ChoicesButtonGuide from "../ChoicesButton/Guide";
import ContractTableGuide from "../ContractTable/Guide";
import DataTableGuide from "../DataTable/Guide";
import { Guide } from "../Guide";
import { IconName } from "../Icon";
import UntypedIconGuide, { IconGuideType } from "../Icon/Guide";
import LinkGuide from "../Link/Guide";
import ModalGuide from "../Modal/Guide";
import ParameterFormGuide from "../ParameterForm/Guide";
import PopoverGuide from "../Popover/Guide";
import SearchInputGuide from "../SearchInput/Guide";
import SelectGuide from "../Select/Guide";
import SidebarLinkGuide from "../SidebarLink/Guide";
import StrongGuide from "../Strong/Guide";
import TableActionBarGuide from "../TableActionBar/Guide";
import { defaultTheme, ThemeProvider } from "../theme";
import TimeInputGuide from "../TimeInput/Guide";
import TruncateGuide from "../Truncate/Guide";

// Specialise IconGuide to the icon names the component library assumes.
const IconGuide = UntypedIconGuide as IconGuideType<IconName>;

const description = `This component guide shows components available for use in
ledger UI applications.`;

const mockRejectPromise = { then: (_: () => void, r: () => void) => r && r() };

// Minimal Apollo client mock
const apolloMock = {
  watchQuery: () => ({
    subscribe: () => undefined,
  }),
  query: () => mockRejectPromise,
  mutate: () => mockRejectPromise,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} as any as ApolloClient<any>;

const App = (_: {}): JSX.Element => (
  <ApolloProvider client={apolloMock}>
    <Guide title="Digital Asset component guide" description={description}>
      <StrongGuide />
      <TruncateGuide />
      <IconGuide names={["chevron-right", "sort-asc", "sort-desc"]} />
      <ButtonGuide />
      <PopoverGuide />
      <ModalGuide />
      <ChoicesButtonGuide />
      <SidebarLinkGuide />
      <BreadcrumbsGuide />
      <SearchInputGuide />
      <SelectGuide />
      <TimeInputGuide />
      <LinkGuide />
      <ContractTableGuide />
      <DataTableGuide />
      <TableActionBarGuide />
      <ArgumentDisplayGuide />
      <ParameterFormGuide />
      <AutosuggestGuide />
    </Guide>
  </ApolloProvider>
);

ReactDOM.render(
  <ThemeProvider theme={defaultTheme}>
    <App />
  </ThemeProvider>,
  document.getElementById("app"),
);
