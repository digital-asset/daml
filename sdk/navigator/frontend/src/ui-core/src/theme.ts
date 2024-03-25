// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as styled from "styled-components";

// This module exports themed styled-components types and a default theme.

/** All style properties that are themable */
export interface ThemeInterface {
  /** Border radius for most elements (e.g., inputs) */
  radiusBorder: string;

  /** Main background color (best approximation if faded) */
  colorBackground: string;

  /** Text and other things on top of background */
  colorForeground: string;

  /**
   * [BackgroundColor, TextColor] for 'primary' elements
   * Used by:
   * - Button[type="primary"]
   * - Table header (left part)
   */
  colorPrimary: [string, string];

  /**
   * [BackgroundColor, TextColor] for 'secondary' elements
   * Used by:
   * - Table header (right part)
   */
  colorSecondary: [string, string];

  /**
   * [BackgroundColor, TextColor] for 'warning' elements
   * Used by:
   * - Button[type="warning"]
   */
  colorWarning: [string, string];

  /**
   * [BackgroundColor, TextColor] for 'danger' elements
   * Used by:
   * - Button[type="danger"]
   */
  colorDanger: [string, string];

  /** Light shade for things like transparent button hover */
  colorShade: string;

  /** Faded ink that should still be readable */
  colorFaded: string;

  /** Background color for input fields */
  colorInputBackground: string;

  colorNavForeground: string;

  /** Navigation: faded ink that should still be readable */
  colorNavFaded: string;

  colorWeakIcon: [string, string];

  /**
   * [BackgroundColor, TextColor] for primary navigation elements
   * Used by:
   * - Button[type="header"]
   * - SidebarLink
   */
  colorNavPrimary: [string, string];

  /**
   * [BackgroundColor, TextColor] for secondary navigation elements
   * Used by:
   */
  colorNavSecondary: [string, string];

  /** Background of the entire document */
  documentBackground: string;

  /** [vertical, horizontal] padding for buttons (space between button text and button border) */
  buttonPadding: [string, string];

  /** Border radius for buttons */
  buttonRadius: string;

  /** Border radius for tooltips/popovers */
  tooltipRadius: string;

  /** Component guide: content size */
  guideWidthMax: string;

  /** Component guide: content size */
  guideWidthMin: string;

  /** Icon prefix for icon fonts (defaults to 'icon-') */
  iconPrefix: string;
}

// Corporate colors, from styleguide
export const White = "#ffffff";
export const Black = "#000000";
export const Grey700 = "#4c566e";
export const Grey600 = "#616a86";
export const Grey500 = "#97acc5";
export const Grey400 = "#ebf2f8";
export const Grey300 = "#f6f9fc";
export const Blue600 = "#64acfc";
export const Blue700 = "#4f93de";
export const DarkBlue300 = "#354c86";
export const DarkBlue600 = "#2b3c68";

export const FadedBlue = "#5e74ac"; // Not in styleguide, sampled from image
export const DarkBlue450 = "#344a83"; // Not in styleguide, sampled from image

export const defaultTheme: ThemeInterface = {
  radiusBorder: "0",
  colorBackground: White,
  colorForeground: Grey600,
  colorPrimary: [Blue600, White],
  colorSecondary: [Blue700, White],
  colorWarning: ["#FFE082", "#333"],
  colorDanger: ["#EF5350", White],
  colorShade: Grey300,
  colorFaded: Grey500,
  colorInputBackground: White,
  colorWeakIcon: [FadedBlue, DarkBlue450],
  colorNavForeground: White,
  colorNavFaded: FadedBlue,
  colorNavPrimary: [DarkBlue600, White],
  colorNavSecondary: [FadedBlue, White],
  documentBackground: `linear-gradient(45deg, ${DarkBlue300}, ${DarkBlue600})`,
  buttonPadding: ["0.5rem", "1rem"],
  buttonRadius: "999em",
  tooltipRadius: "4px",
  guideWidthMax: "1000px",
  guideWidthMin: "500px",
  iconPrefix: "icon-",
};

/**
 * Style properties that are shared between components,
 * but are hardcoded. In the future, we may want to consolidate
 * some of them and move them to the actual theme.
 */
export const hardcodedStyle = {
  /** Font size for buttons */
  buttonFontSize: "1.0rem",

  /** The height of the text element inside a button (depends on buttonFontSize) */
  buttonTextHeight: "1.25rem",

  /** Drop shadow for buttons */
  buttonShadow: "0 0 0 1px rgba(16,22,26,0.1), 0 2px 4px rgba(16,22,26,0.2)",

  /** Font weight for labels (and column headers) */
  labelFontWeight: "400",

  /** Text transform for labels (and column headers) */
  labelTextTransform: "none",

  /** Height of page headers (and table action bars) */
  pageHeaderHeight: "5rem",

  /** Table rows: height, in pixels */
  tableRowHeight: 50,

  /** Table column headers: height, in pixels */
  tableHeaderHeight: 56,

  /** Width of indicator of hovered row */
  tableHoverBorderWidth: "6px",

  /** Background color for hovered rows */
  tableHoverBackgroundColor: "#E7EDf3",

  /** Background color for even rows */
  tableStripeBackgroundColor: Grey300,

  /** Margin between table cells */
  tableCellHorizontalMargin: "0.625rem",

  /** Left and right margins */
  tableSideMargin: "2.5rem",

  /** Style of rows for appearing contracts */
  tableRowAppear: `color: ${Blue600};`,

  /** Style of rows for disappearing contracts */
  tableRowDisappear: `color: #A4B9CB;`,

  /** Style of rows for archived contracts */
  tableRowArchived: `color: #AAA`,

  /** Nested forms indent */
  formNestedIndent: "1.0rem",

  /** Nested forms tree rendering */
  formNestedLeftBorder: `1px solid ${Grey400}`,

  /** Margin between table action bar elements */
  actionBarElementMargin: "1rem",

  /** Width of the left sidebar */
  sidebarWidth: "16rem",

  /** Font size in sidebar links */
  sidebarFontSize: "0.85rem",

  /** Small icon for displaying numbers (e.g., in sidebar links) */
  smallNumberIcon: `
    font-size: 0.85rem;
    margin-left: 0.5rem;
    border-radius: 0.85rem;
    padding: 0.25rem 0.5rem;
    display: flex;
    justify-content: center;
    align-items: center;
  `,

  /** Default time format (moment.js format string) */
  defaultTimeFormat: "YYYY-MM-DD HH:mm",

  /** Default time format (moment.js format string) */
  defaultDateFormat: "YYYY-MM-DD",
};

// Re-export styled components parameterised with the theme type. This
// particular patterns seems to have TypeScript produce the correct declaration
// files. Some others didn't.

const typed: styled.ThemedStyledComponentsModule<ThemeInterface> =
  styled as styled.ThemedStyledComponentsModule<ThemeInterface>;

export const {
  css,
  default: typedStyled,
  keyframes,
  ThemeProvider,
  withTheme,
} = typed;

export default typedStyled;
