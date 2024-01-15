// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Dispatch, Route } from "@da/ui-core";
import UntypedLink, { HrefTarget } from "@da/ui-core/lib/Link";
import * as React from "react";
import { connect, ConnectedComponent } from "react-redux";
import * as App from "../applets/app";
import { pathToAction } from "../routes";

// Note: The react-redux typings for connect() try to remove
// the 'dispatch' property from OwnProps. This makes sense since react-redux
// removes that property at runtime. It is therefore not a good idea to
// allow arbitrary properties via '[key: string]: any'.

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface OwnProps<P = any> {
  route: Route<P, App.Action, App.State>;
  params: P;
  className?: string;
  target?: HrefTarget;
}

interface ReduxProps {
  dispatch: Dispatch<App.Action>;
}

type Props = ReduxProps & OwnProps;

class Link extends React.Component<Props, {}> {
  constructor(props: Props) {
    super(props);
    this.click = this.click.bind(this);
  }

  click(e: React.MouseEvent<HTMLAnchorElement>) {
    this.props.dispatch(pathToAction(e.currentTarget.pathname));
  }

  render(): React.ReactElement<HTMLAnchorElement> {
    const { route, className, target, children, params } = this.props;

    const href = route.render(params);

    return (
      <UntypedLink
        onClick={this.click}
        href={href}
        className={className}
        target={target}>
        {children}
      </UntypedLink>
    );
  }
}

const C: ConnectedComponent<
  typeof Link,
  React.PropsWithChildren<OwnProps>
> = connect()(Link);
export default C;
