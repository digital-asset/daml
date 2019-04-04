CHANGELOG
=========

## 0.22.0
- Add support for dates without a time
- Numbers are encoded as strings in the argument JSON

## 0.21.2
- Fixed an issue with TimeInput where the date selector would
  disappear on click.

## 0.21.0
- Added support for different time modes
- Add support for DAML's new builtin Unit type

## 0.20.1
- Add BUCK file to .npmignore

## 0.20.0

- Fixed flickering of tables in Firefox.
- All dates now display in the same format (YYYY-MM-DD HH:mm)
- Add Modal component
- Add Select component
- Add withLedgerTime higher order component
- Add withExercise higher order component
- Minor changes to the visual style of several components
- DateTimePicker changes always need to be confirmed with
  the 'Set' button. This prevents issues where the time of
  day can not be lowered.

## 0.19.1

- NavBar logo can now be any element, instead of just an image URL.

## 0.19.0

- Add TableActionBar component.
- Add 'inverted-primary' button style.

## 0.18.0

- Add basic support for list, variant, and record parameters.
- Introduce PartialArgument to improve typing of partially specified
  arguments.
- Button now specifies the button type, to prevent automatic
  submission of forms containing buttons.

## 0.17.2

- Add filter functionality to contract and data tables.

## 0.17.1

- Upgrade session to be compatible with ui-backend 0.4.0

## 0.17.0

- Redesigned the theme interface (header colors are now part of the
  theme, apps no longer need two themes for the header and the content).
- Add NavBar component
- Add Frame component
- Add AdvanceTime component
- Add ChoicesButton component
- Add Truncate component

## 0.16.1

- Fixed broken ledger watcher icon.

## 0.16.0

- Restyle all components according to corporate design.

## 0.15.0

- Add DateTimePicker component.
- Add DataTable component.
- Add ledger-watcher applet.

## 0.14.2

- Add Popover component.

## 0.14.1

- Add TimeInput component.

## 0.14.0

- Decimals and numbers are now encoded as numbers in the argument JSON.

## 0.13.1

- No change from 0.13.0, but released through Jenkins pipeline.

## 0.13.0

- The @da/navigator-types library has been merged into @da/ui-core and
  explict imports have been added to the code in @da/ui-core.

## 0.12.1

- Fixed broken dependencies in 0.12.0 (the dependencies from the
  @da/route-matcher library were added as devDependencies instead of
  hard dependencies by mistake).

## 0.12.0

- The @da/route-matcher library has been merged into @da/ui-core.

## 0.11.0

- The @da/ledger-watcher library has been merged into @da/ui-core.

## 0.10.0

- Renamed from @da/components to @da/ui-core.
- First release on Artifactory under the new name.

## 0.9.5

- Whitespace-only change in package metadata.

## 0.9.4

- Include accidentally ignored Icon files.

## 0.9.3

- Adjusted breadcrumbs divider styling.
- Added ability to extend contract table config type.

## 0.9.0

- Update button styling.
- Change some theme color variables to be tuples (background color, foreground
  color). This is a breaking change.

## 0.8.5

- Add a `makeSidebarLink` component factory.
- Revert dependency bumps for now because of type problems.

## 0.6.9

- Show all three icons used in the component guide.
- Set contract table width to 100%.

## 0.6.8

- Upgrade to 2.1 of `styled-components`.

## 0.6.6

- Make `dataProvider` in `ParameterFiorm` optional.
