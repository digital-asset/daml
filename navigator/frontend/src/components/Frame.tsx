// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Frame as CoreFrame, makeSidebarLink } from "@da/ui-core";
import * as LedgerWatcher from "@da/ui-core/lib/ledger-watcher";
import * as Session from "@da/ui-core/lib/session";
import * as React from "react";
import * as App from "../applets/app";
import * as ConfigSource from "../applets/configsource";
import * as Page from "../applets/page";
import { ConfigType } from "../config";
import * as Routes from "../routes";
import Link from "./Link";
import Navbar from "./Navbar";

const Item = makeSidebarLink(Link);

interface FrameProps {
  user: Session.User;
  page: Page.State;
  watcher: LedgerWatcher.State;
  configSource: ConfigSource.State;
  config: ConfigType;
  toConfig(action: ConfigSource.Action): App.Action;
  toSession(action: Session.Action): App.Action;
  toPage(action: Page.Action): App.Action;
  toWatcher(action: LedgerWatcher.Action): App.Action;
}

const Frame: React.FC<FrameProps> = ({
  user,
  page,
  watcher,
  configSource,
  config,
  toConfig,
  toSession,
  toPage,
  toWatcher,
}: FrameProps) => {
  const activeRoute = Routes.activeRoute();

  return (
    <CoreFrame
      top={
        <Navbar
          user={user}
          watcher={watcher}
          toSession={toSession}
          toWatcher={toWatcher}
        />
      }
      left={[
        <Item
          title="Contracts"
          isActive={activeRoute === Routes.contracts}
          route={Routes.contracts}
          params={{}}
          key="Contracts"
        />,
        <Item
          title="Templates"
          isActive={activeRoute === Routes.templates}
          route={Routes.templates}
          params={{}}
          key="Templates"
        />,
        ...Object.keys(config.customViews).map(id => {
          const { title } = config.customViews[id];
          return (
            <Item
              title={title}
              isActive={page.type === "customview" && page.state.id === id}
              route={Routes.customView}
              params={{ id }}
              key={`Custom-${id}`}
            />
          );
        }),
      ]}
      content={
        <Page.UI
          toConfig={toConfig}
          toSelf={toPage}
          toWatcher={toWatcher}
          page={page}
          user={user}
          configSource={configSource}
          config={config}
        />
      }
    />
  );
};

export default Frame;
