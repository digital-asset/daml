// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// The CSS style (but not the code) of the tooltip is based on the
// blueprintjs library.
// See https://github.com/palantir/blueprint.
// The license of this library is included in the current folder.
import * as React from "react";
import { Placement } from "../Popover";
import styled from "../theme";

const arrowSize = 11;
const arrowCrossDist = 2 * arrowSize;

function getMargin({ margin, arrow }: StyleProps) {
  return `${arrow ? arrowSize + margin : margin}px`;
}

const hCenter = `
  top: 50%;
  transform: translateY(-50%);
`;

const hStart = `
  top: ${arrowCrossDist}px;
  transform: translateY(-${arrowCrossDist - 4}px);
`;

const hEnd = `
  bottom: ${arrowCrossDist}px;
  transform: translateY(${arrowCrossDist - 4}px);
`;

const vCenter = `
  left: 50%;
  transform: translateX(-50%);
`;

const vStart = `
  left: ${arrowCrossDist}px;
  transform: translateX(-${arrowCrossDist - 4}px);
`;

const vEnd = `
  right: ${arrowCrossDist}px;
  transform: translateX(${arrowCrossDist - 4}px);
`;

function getArrowAttachment(placement: Placement) {
  switch (placement) {
    case "left":
      return `right: -${arrowSize}px;${hCenter}`;
    case "left-start":
      return `right: -${arrowSize}px;${hStart}`;
    case "left-end":
      return `right: -${arrowSize}px;${hEnd}`;
    case "right":
      return `left: -${arrowSize}px;${hCenter}`;
    case "right-start":
      return `left: -${arrowSize}px;${hStart}`;
    case "right-end":
      return `left: -${arrowSize}px;${hEnd}`;
    case "top":
      return `bottom: -${arrowSize}px;${vCenter}`;
    case "top-start":
      return `bottom: -${arrowSize}px;${vStart}`;
    case "top-end":
      return `bottom: -${arrowSize}px;${vEnd}`;
    case "bottom":
      return `top: -${arrowSize}px;${vCenter}`;
    case "bottom-start":
      return `top: -${arrowSize}px;${vStart}`;
    case "bottom-end":
      return `top: -${arrowSize}px;${vEnd}`;
  }
}

function getArrowTransform(placement: Placement) {
  switch (placement) {
    case "left":
    case "left-start":
    case "left-end":
      return `rotate(180deg)`;
    case "right":
    case "right-start":
    case "right-end":
      return `rotate(0deg)`;
    case "top":
    case "top-start":
    case "top-end":
      return `rotate(-90deg)`;
    case "bottom":
    case "bottom-start":
    case "bottom-end":
      return `rotate(90deg)`;
  }
}

const StyledArrow = styled.div<{ placement: Placement }>`
  position: absolute;
  ${props => getArrowAttachment(props.placement)}
  width: 30px;
  height: 30px;

  ::before {
    box-shadow: 1px 1px 6px rgba(16, 22, 26, 0.2);
    margin: 5px;
    width: 20px;
    height: 20px;
    display: block;
    position: absolute;
    transform: rotate(45deg);
    border-radius: 2px;
    content: "";
  }

  .border {
    fill: #10161a;
    fill-opacity: 0.1;
  }

  .fill {
    fill: ${({ theme }) => theme.colorBackground};
  }

  .svg {
    transform: ${props => getArrowTransform(props.placement)};
  }
`;

const borderPath = `M8.11 6.302c1.015-.936 1.887-2.922
1.887-4.297v26c0-1.378-.868-3.357-1.888-4.297L.925
17.09c-1.237-1.14-1.233-3.034 0-4.17L8.11 6.302z`;

const fillPath = `M8.787 7.036c1.22-1.125 2.21-3.376
2.21-5.03V0v30-2.005c0-1.654-.983-3.9-2.21-5.03l-7.183-6.616c-.81-.746-.802-1.96
0-2.7l7.183-6.614z`;

function Arrow(props: StyleProps) {
  if (props.arrow) {
    return (
      <StyledArrow {...props}>
        <svg className="svg" viewBox="0 0 30 30">
          <path className="border" d={borderPath} />
          <path className="fill" d={fillPath} />
        </svg>
      </StyledArrow>
    );
  } else {
    return null;
  }
}

function getArrowMargin(props: StyleProps) {
  switch (props.placement) {
    case "left":
    case "left-start":
    case "left-end":
      return `0 ${getMargin(props)} 0 0`;
    case "right":
    case "right-start":
    case "right-end":
      return `0 0 0 ${getMargin(props)}`;
    case "top":
    case "top-start":
    case "top-end":
      return `0 0 ${getMargin(props)} 0`;
    case "bottom":
    case "bottom-start":
    case "bottom-end":
      return `${getMargin(props)} 0 0 0`;
  }
}

const ContentOuterContainer = styled.div<StyleProps>`
  box-shadow: 0 0 0 1px rgba(16, 22, 26, 0.1), 0 2px 4px rgba(16, 22, 26, 0.2),
    0 8px 24px rgba(16, 22, 26, 0.2);
  transform: scale(1);
  display: inline-block;
  z-index: 20;
  border-radius: ${({ theme }) => theme.tooltipRadius};
  margin: ${props => getArrowMargin(props)};
`;

const ContentInnerContainer = styled.div`
  position: relative;
  padding: 0;
  border-radius: ${({ theme }) => theme.tooltipRadius};
  background-color: ${({ theme }) => theme.colorBackground};
`;
// ----------------------------------------------------------------------------
// Tooltip component
// ----------------------------------------------------------------------------

export interface StyleProps {
  /** Placement, relative to target */
  placement: Placement;

  /** Whether to display an arrow */
  arrow: boolean;

  /** Additional margin, in pixels */
  margin: number;

  onClick?(): void;
}

export interface OtherProps {
  /** Content of the tooltip */
  children: React.ReactChild;
  forwardedRef: React.Ref<HTMLDivElement>;
}

export type Props = StyleProps & OtherProps;

class Tooltip extends React.Component<Props> {
  render() {
    const { children, forwardedRef, ...otherProps } = this.props;

    return (
      <ContentOuterContainer
        {...otherProps}
        onClick={this.props.onClick}
        ref={forwardedRef}>
        <Arrow {...otherProps} />
        <ContentInnerContainer>{children}</ContentInnerContainer>
      </ContentOuterContainer>
    );
  }
}

export default React.forwardRef<HTMLDivElement, Omit<Props, "forwardedRef">>(
  (props, ref) => <Tooltip {...props} forwardedRef={ref} />,
);
