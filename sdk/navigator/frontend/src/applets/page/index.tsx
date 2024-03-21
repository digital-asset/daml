// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as LedgerWatcher from "@da/ui-core/lib/ledger-watcher";
import * as Session from "@da/ui-core/lib/session";
import * as React from "react";
import { ConfigType } from "../../config";
import * as About from "../about";
import * as App from "../app";
import * as ConfigSource from "../configsource";
import * as Contract from "../contract";
import * as Contracts from "../contracts";
import * as CustomView from "../customview";
import * as Info from "../info";
import * as Template from "../template";
import * as TemplateContracts from "../templatecontracts";
import * as Templates from "../templates";

export type State =
  | { type: "configsource" }
  | { type: "about"; state: About.State }
  | { type: "contracts"; state: Contracts.State }
  | { type: "contract"; state: Contract.State }
  | { type: "customview"; state: CustomView.State }
  | { type: "info"; state: Info.State }
  | { type: "template"; state: Template.State }
  | { type: "templates"; state: Templates.State }
  | { type: "templatecontracts"; state: TemplateContracts.State };

// The page part of the state accepts a SET action to replace the current page
// and actions that send actions to particular pages.
export type Action =
  | { type: "SET"; page: State }
  | { type: "TO_ABOUT"; action: About.Action }
  | { type: "TO_CONTRACT"; action: Contract.Action }
  | { type: "TO_CONTRACTS"; action: Contracts.Action }
  | { type: "TO_CUSTOMVIEW"; action: CustomView.Action }
  | { type: "TO_INFO"; action: Info.Action }
  | { type: "TO_TEMPLATE"; action: Template.Action }
  | { type: "TO_TEMPLATES"; action: Templates.Action }
  | { type: "TO_TEMPLATECONTRACTS"; action: TemplateContracts.Action };

// Action creators
export const setPage = (page: State): Action => ({ type: "SET", page });
export const toAbout = (action: About.Action): Action => ({
  type: "TO_ABOUT",
  action,
});
export const toContract = (action: Contract.Action): Action => ({
  type: "TO_CONTRACT",
  action,
});
export const toContracts = (action: Contracts.Action): Action => ({
  type: "TO_CONTRACTS",
  action,
});
export const toCustomView = (action: CustomView.Action): Action => ({
  type: "TO_CUSTOMVIEW",
  action,
});
export const toInfo = (action: Info.Action): Action => ({
  type: "TO_INFO",
  action,
});
export const toTemplate = (action: Template.Action): Action => ({
  type: "TO_TEMPLATE",
  action,
});
export const toTemplates = (action: Templates.Action): Action => ({
  type: "TO_TEMPLATES",
  action,
});
export const toTemplateContracts = (
  action: TemplateContracts.Action,
): Action => ({ type: "TO_TEMPLATECONTRACTS", action });

export const reduce = (page?: State, action?: Action): State => {
  if (page === undefined || action === undefined) {
    // Return the initial page
    return { type: "contracts", state: Contracts.init() };
  }

  // Route actions to the correct page and ignore if not active.
  if (action.type === "SET") {
    return action.page;
  } else if (action.type === "TO_ABOUT" && page.type === "about") {
    return { ...page, state: About.reduce(page.state, action.action) };
  } else if (action.type === "TO_TEMPLATE" && page.type === "template") {
    return { ...page, state: Template.reduce(page.state, action.action) };
  } else if (action.type === "TO_CONTRACT" && page.type === "contract") {
    return { ...page, state: Contract.reduce(page.state, action.action) };
  } else if (action.type === "TO_CONTRACTS" && page.type === "contracts") {
    return { ...page, state: Contracts.reduce(page.state, action.action) };
  } else if (action.type === "TO_CUSTOMVIEW" && page.type === "customview") {
    return { ...page, state: CustomView.reduce(page.state, action.action) };
  } else if (action.type === "TO_INFO" && page.type === "info") {
    return { ...page, state: Info.reduce(page.state, action.action) };
  } else if (action.type === "TO_TEMPLATES" && page.type === "templates") {
    return { ...page, state: Templates.reduce(page.state, action.action) };
  } else if (
    action.type === "TO_TEMPLATECONTRACTS" &&
    page.type === "templatecontracts"
  ) {
    return {
      ...page,
      state: TemplateContracts.reduce(page.state, action.action),
    };
  } else {
    return page;
  }
};

export interface Props {
  page: State;
  configSource: ConfigSource.State;
  config: ConfigType;
  user: Session.User;
  toSelf(action: Action): App.Action;
  toWatcher(action: LedgerWatcher.Action): App.Action;
  toConfig(action: ConfigSource.Action): App.Action;
}

const Component: React.FC<Props> = ({
  configSource,
  page,
  user,
  config,
  toConfig,
  toSelf,
  toWatcher,
}) => {
  switch (page.type) {
    case "about":
      return (
        <About.UI
          state={page.state}
          toSelf={(action: About.Action) => toSelf(toAbout(action))}
        />
      );
    case "info":
      return (
        <Info.UI
          state={page.state}
          toSelf={(action: Info.Action) => toSelf(toInfo(action))}
        />
      );
    case "template":
      return (
        <Template.UI
          state={page.state}
          toSelf={(action: Template.Action) => toSelf(toTemplate(action))}
          toWatcher={toWatcher}
        />
      );
    case "contract":
      return (
        <Contract.UI
          state={page.state}
          toSelf={(action: Contract.Action) => toSelf(toContract(action))}
          toWatcher={toWatcher}
        />
      );
    case "contracts":
      return (
        <Contracts.UI
          toSelf={(action: Contracts.Action) => toSelf(toContracts(action))}
          state={page.state}
          user={user}
        />
      );
    case "customview":
      return (
        <CustomView.UI
          toSelf={(action: CustomView.Action) => toSelf(toCustomView(action))}
          toWatcher={toWatcher}
          state={page.state}
          user={user}
          config={config}
        />
      );
    case "templates":
      return (
        <Templates.UI
          toSelf={(action: Templates.Action) => toSelf(toTemplates(action))}
          state={page.state}
          user={user}
        />
      );
    case "templatecontracts":
      return (
        <TemplateContracts.UI
          toSelf={(action: TemplateContracts.Action) =>
            toSelf(toTemplateContracts(action))
          }
          state={page.state}
          user={user}
        />
      );
    case "configsource":
      return (
        <ConfigSource.UI
          toSelf={(action: ConfigSource.Action) => toConfig(action)}
          state={configSource}
          config={config}
          user={user}
        />
      );
  }
};

export const UI = Component;
