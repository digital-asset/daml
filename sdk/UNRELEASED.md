# Release of Daml SDK DAML_VERSION

Daml SDK DAML_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Tuesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

## Until 2026-05-05 (Exclusive)
- adopted LF 2.3
- Removed daml assistant

## Until 2026-04-14 (Exclusive)
- Added --list-scripts-json flag to `dpm script`, for listing all script names in a DAR
- Update to DPM 1.0.12

## Until 2026-03-31 (Exclusive)
- Codegen-java: Added support for `UnknownTrailingFieldPolicy` in the generated `fromCreatedEvent()` method.
- Support NUCK in IDE ledger
- Add --ide-ledger-protocol-version flag to Daml Script CLI runner
- Add `script-service.protocol-version` to daml.yaml for Daml Test and Daml Studio
- (From 2.10 keys to 3.5) Renamed daml-script `queryContractKey` to `queryByKey`. Added `queryNByKey` and `queryAllByKey`

## Until 2026-03-24 (Exclusive)
- Daml Script: Handle new EffectfulRollback errors from the Ledger API
- Moved Contract keys and LookupNByKey to 2.3-staging (from 2.dev)

## Until 2026-03-17 (Exclusive)

- Codegen-java: Added support for `UnknownTrailingFieldPolicy` in the generated `valueDecoder()` and `fromJson()` methods.

## Until 2026-03-10 (Exclusive)
- `DA.Crypto.Text` is now marked as stable
- Added `explicit-serializable` build option to opt in to explicit `Serializable` instances

## Until 2025-12-11 (Exclusive)
- Changed java codegen to drop methods deprecated since 2.3.0. 
  Given template `Pizza` and choice `Bake`, Following methods will no longer be generated:
  ```
  createAndExerciseArchive(Archive arg)
  createAndExerciseArchive()
  createAndExerciseBake(Pizza_Bake arg)
  createAndExerciseBake(String pizzaiolo)
  exerciseByKeyArchive(PizzaKey key, Archive arg)
  exerciseByKeyArchive(PizzaKey key)
  exerciseByKeyBake(PizzaKey key, Pizza_Bake arg)
  exerciseByKeyBake(PizzaKey key, String pizzaiolo)
  ```

## Until 2025-12-04 (Exclusive)
- Added `templateIdWithPackageId` in generated typescript instances of `Template` and `InterfaceCompanion`.

## Until 2025-10-16 (Exclusive)
- `unsafeFromInterface` method in `HasFromInterface` type class is deprecated.
  Its new implementation throws an error at run time. User should use the upgrade-aware `fromInterface` instead.
  `UNSAFE_FROM_INTERFACE` primitive is removed, starting from Daml LF 2.2 included.


## Until 2025-09-18 (Exclusive)
- typescript codegen has been changed to produce the interface definitions that use package id rather than package name.
  Following definition
  ```
  exports.Token = damlTypes.assembleInterface(
    '41d34898b0a96f443eef3fa59e0ca61465caa5e1c2ca24ecb8e7de5e1ba611f5:Main:Token',
    function () { return exports.EmptyInterfaceView; },
    {
    ...
  ```
  becomes
  ```
  exports.Token = damlTypes.assembleInterface(
    '#tokens:Main:Token',
    function () { return exports.EmptyInterfaceView; },
    {
  ```

