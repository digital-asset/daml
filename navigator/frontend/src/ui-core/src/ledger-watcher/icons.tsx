// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { keyframes } from "styled-components";
import { WatchedCommand } from ".";
import UntypedIcon from "../Icon";
import styled from "../theme";

export const fadeTime = 1000;

const rotate360 = keyframes`
  from {
    transform: rotate(0deg);
  }

  to {
    transform: rotate(360deg);
  }
`;

export interface SpinnerProps {
  size: number;
  borderSize: number;
}

const gray = "rgba(255, 255, 255, 0.2)";

const Spinner = styled.div<SpinnerProps>`
  border-radius: 50%;
  width: ${props => props.size}px;
  height: ${props => props.size}px;
  position: relative;
  border-top: ${props => props.borderSize}px solid ${gray};
  border-right: ${props => props.borderSize}px solid ${gray};
  border-bottom: ${props => props.borderSize}px solid ${gray};
  border-left: ${props => props.borderSize}px solid #ffffff;
  transform: translateZ(0);
  animation: ${rotate360} 0.5s infinite linear;
`;

const fadeAnimation = keyframes`
  0% {opacity: 1;}
  25% {opacity: 0.9;}
  50% {opacity: 0.7;}
  75% {opacity: 0.4;}
  100% {opacity: 0;}
`;

const NormalWrapper = styled.div``;
const FadedWrapper = styled.div`
  animation: ${fadeAnimation} ${fadeTime / 1000}s linear;
  opacity: 0;
`;

const IconSuccess = () => <UntypedIcon name="check" />;
const IconError = () => <UntypedIcon name="error" />;
const IconWaiting = () => <Spinner size={24} borderSize={4} />;

export function createIcon(cmd: WatchedCommand, fade: boolean): JSX.Element {
  const IconWrapper = fade ? FadedWrapper : NormalWrapper;
  if (cmd.result && cmd.result.type === "SUCCESS") {
    return (
      <IconWrapper key={cmd.commandId}>
        <IconSuccess />
      </IconWrapper>
    );
  } else if (cmd.result && cmd.result.type === "ERROR") {
    return (
      <IconWrapper key={cmd.commandId}>
        <IconError />
      </IconWrapper>
    );
  } else {
    return (
      <IconWrapper key={cmd.commandId}>
        <IconWaiting />
      </IconWrapper>
    );
  }
}
