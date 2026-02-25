-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
module DA.Cli.Options
  ( module DA.Cli.Options
  ) where

import Data.List.Extra     (lower, splitOn, trim)
import Options.Applicative hiding (option, strOption)
import qualified Options.Applicative (option, strOption)
import Options.Applicative.Extended
import Data.List
import Data.Maybe
import qualified DA.Pretty           as Pretty
import DA.Daml.Helper.Studio (ReplaceExtension (..))
import DA.Daml.Options.Types
import DA.Daml.LF.Ast.Util (splitUnitId)
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import qualified DA.Service.Logger as Logger
import qualified Module as GHC
import qualified Text.ParserCombinators.ReadP as R
import qualified Data.Text as T
import DA.Daml.LF.TypeChecker.Error.WarningFlags
import Data.HList
import qualified DA.Daml.LF.TypeChecker.Error as TypeCheckerError
import qualified DA.Daml.LFConversion.Errors as LFConversion

-- | Pretty-printing documents with syntax-highlighting annotations.
type Document = Pretty.Doc Pretty.SyntaxClass

-- | Flags
newtype DontDivulgeContractIdsInCreateArguments = DontDivulgeContractIdsInCreateArguments Bool
newtype DontDiscloseNonConsumingChoicesToObservers = DontDiscloseNonConsumingChoicesToObservers Bool

-- | Document rendering styles for console output.
data Style
  = Plain
  | Colored

-- | Rendering a pretty-printed document.
render :: Style -> Document -> String
render s d = resolve s d
  where
    resolve = \case
      Plain   -> Pretty.renderPlain
      Colored -> Pretty.renderColored

inputFileOptWithExt :: String -> Parser FilePath
inputFileOptWithExt extension = argument str $
       metavar "FILE"
    <> help ("Input " <> extension <> " file whose contents are read")

inputFileOpt, inputDarOpt, inputFileRstOpt, inputDalfOpt :: Parser FilePath
inputFileOpt = inputFileOptWithExt ".daml"
inputDarOpt = inputFileOptWithExt ".dar"
inputDalfOpt = inputFileOptWithExt ".dalf"
inputFileRstOpt = inputFileOptWithExt ".rst"

targetSrcDirOpt :: Parser (Maybe FilePath)
targetSrcDirOpt =
    optionOnce (Just <$> str) $
    metavar "TARGET_SRC_DIR"
    <> help "Optional target directory to write created sources to"
    <> long "srcdir"
    <> value Nothing

qualOpt :: Parser (Maybe String)
qualOpt =
    optionOnce (Just <$> str) $
    metavar "QUALIFICATION" <>
    help "Optional qualification to append to generated module name." <>
    long "qualify" <>
    value Nothing

outputFileOpt :: Parser String
outputFileOpt = strOptionOnce $
       metavar "FILE"
    <> help "Output file (use '-' for stdout)"
    <> short 'o'
    <> long "output"
    <> value "-"

optionalOutputFileOpt :: Parser (Maybe String)
optionalOutputFileOpt = optionOnce (Just <$> str) $
       metavar "FILE"
    <> help "Optional output file (defaults to <PACKAGE-NAME>.dar)"
    <> short 'o'
    <> long "output"
    <> value Nothing

lfVersionOpt :: Parser LF.Version
lfVersionOpt = optionOnce (str >>= select) $
       metavar "DAML-LF-VERSION"
    <> help ("Daml-LF version to output: " ++ versionsStr)
    <> long "target"
    <> value LF.defaultLfVersion
    <> internal
  where
    renderVersion v =
      let def = if v == LF.defaultLfVersion then " (default)" else ""
      in Pretty.renderPretty v ++ def
    versionsStr = intercalate ", " (map renderVersion versions)
    select = \case
      versionStr
        | Just version <- LF.parseVersion versionStr
        , version `elem` versions
        -> return version
        | otherwise
        -> readerError $ "Unknown Daml-LF version: " ++ versionStr ++ ", accepted versions: " ++ (show $ map LF.renderVersion versions)
    versions = LF.compilerOutputLfVersions

dotFileOpt :: Parser (Maybe FilePath)
dotFileOpt = optionOnce (Just <$> str) $
       metavar "FILE"
    <> help "Name of the dot file to be generated."
    <> long "dot"
    <> value Nothing

htmlOutFile :: Parser FilePath
htmlOutFile = strOptionOnce $
    metavar "FILE"
    <> help "Name of the HTML file to be generated"
    <> short 'o'
    <> long "output"
    <> value "visual.html"

-- switch' if a value is not present it is assumed to be be true, while switch assumes it to be false
switch' :: Mod FlagFields Bool -> Parser Bool
switch' = flag True False

openBrowser :: Parser Bool
openBrowser = switch' $
       long "verbose"
    <> short 'b'
    <> help "Open Browser after generating D3 visualization, defaults to true"

newtype Debug = Debug Bool
debugOpt :: Parser Debug
debugOpt = fmap Debug $
    switch $
       long "debug"
    <> short 'd'
    <> help "Enable debug output."

newtype InitPkgDb = InitPkgDb Bool
initPkgDbOpt :: Parser InitPkgDb
initPkgDbOpt = InitPkgDb <$> flagYesNoAuto "init-package-db" True "Initialize package database" idm

newtype EnableMultiPackage = EnableMultiPackage {getEnableMultiPackage :: Bool}
enableMultiPackageOpt :: Parser EnableMultiPackage
enableMultiPackageOpt = EnableMultiPackage <$> flagYesNoAuto "enable-multi-package" True "Enable/disable multi-package.yaml support (enabled by default)" idm

newtype MultiPackageBuildAll = MultiPackageBuildAll {getMultiPackageBuildAll :: Bool}
multiPackageBuildAllOpt :: Parser MultiPackageBuildAll
multiPackageBuildAllOpt = MultiPackageBuildAll <$> switch (long "all" <> help "Build all packages in multi-package.yaml")

newtype MultiPackageNoCache = MultiPackageNoCache {getMultiPackageNoCache :: Bool}
multiPackageNoCacheOpt :: Parser MultiPackageNoCache
multiPackageNoCacheOpt = MultiPackageNoCache <$> switch (long "no-cache" <> help "Disables cache checking, rebuilding all dependencies")

data MultiPackageLocation
  -- | Search for the multi-package.yaml above the current directory
  = MPLSearch
  -- | Expect the multi-package.yaml at the given path
  | MPLPath FilePath
  deriving (Show, Eq)

multiPackageLocationOpt :: Parser MultiPackageLocation
multiPackageLocationOpt =
  optionOnce (MPLPath <$> str)
    (  metavar "FILE"
    <> help "Path to the multi-package.yaml file"
    <> long "multi-package-path"
    <> value MPLSearch
    )

newtype MultiPackageCleanAll = MultiPackageCleanAll {getMultiPackageCleanAll :: Bool}
multiPackageCleanAllOpt :: Parser MultiPackageCleanAll
multiPackageCleanAllOpt = MultiPackageCleanAll <$> switch (long "all" <> help "Clean all packages in multi-package.yaml")

data Telemetry
    = TelemetryOptedIn -- ^ User has explicitly opted in
    | TelemetryOptedOut -- ^ User has explicitly opted out
    | TelemetryIgnored -- ^ User has clicked away the telemetry dialog without making a choice
    | TelemetryDisabled -- ^ No options have been supplied so telemetry is
               -- disabled. You’ll never get this in the IDE but it is
               -- used when invoking the compiler from a terminal.

newtype GenerateMultiPackageManifestOutput = GenerateMultiPackageManifestOutput {getGenerateMultiPackageManifestOutput :: Maybe FilePath}
generateMultiPackageManifestOutputOpt :: Parser GenerateMultiPackageManifestOutput
generateMultiPackageManifestOutputOpt = fmap GenerateMultiPackageManifestOutput $ optional $ optionOnce str
    (  metavar "FILE"
    <> help "File to write the manifest to (JSON)"
    <> long "output"
    )

telemetryOpt :: Parser Telemetry
telemetryOpt = fromMaybe TelemetryDisabled <$> optional (optIn <|> optOut <|> optIgnored)
  where
    optIn = flag' TelemetryOptedIn $ hidden <> long "telemetry"
    optOut = flag' TelemetryOptedOut $ hidden <> long "optOutTelemetry"
    optIgnored = flag' TelemetryIgnored $ hidden <> long "telemetry-ignored"

-- Parse helper for non-empty string lists separated by the given separator
stringsSepBy :: Char -> ReadM [String]
stringsSepBy sep = eitherReader sepBy'
  where sepBy' :: String -> Either String [String]
        sepBy' input
          | null items = Left "Failed to read items: empty list"
          | any null items = Left $ "Failed to read items: empty item within " <> input
          | otherwise = Right items
          where
            items = map trim $ splitOn [sep] input

data PackageLocationOpts = PackageLocationOpts
    { packageRoot :: Maybe PackagePath
    -- ^ An explicit package path specified by the user.
    , packageLocationCheck :: PackageLocationCheck
    -- ^ Throw an error if this is not run in a package.
    }

packageLocationOpts :: String -> Parser PackageLocationOpts
packageLocationOpts name = PackageLocationOpts <$> packageOrProjectRootOpt <*> packageOrProjectLocationCheckOpt name
    where
        packageRootOptDesc :: [String]
        packageRootOptDesc =
            [ "Path to the root of a package containing daml.yaml. "
            , "You should prefer the DAML_PACKAGE environment variable over this option."
            , "See https://docs.digitalasset.com/build/3.4/dpm/configuration.html#configuration-options for more details."
            ]
        packageOrProjectRootOpt :: Parser (Maybe PackagePath)
        packageOrProjectRootOpt = optional $ packageRootOpt <|> projectRootOpt
        projectRootOpt :: Parser PackagePath
        projectRootOpt =
            fmap PackagePath $
            strOptionOnce $
            long "project-root" <> hidden <>
            help (mconcat $ packageRootOptDesc ++ ["(project-root is deprecated, please use --package-root)"])
        packageRootOpt :: Parser PackagePath
        packageRootOpt =
            fmap PackagePath $
            strOptionOnce $
            long "package-root" <>
            help (mconcat packageRootOptDesc)
        packageOrProjectLocationCheckOpt cmdName = packageLocationCheckOpt cmdName <|> projectLocationCheckOpt cmdName
        projectLocationCheckOpt cmdName = fmap (PackageLocationCheck cmdName) . switch $
               help "Check if running in Daml package.\n(project-check is deprecated, please use --package-check)"
            <> long "project-check" <> internal
        packageLocationCheckOpt cmdName = fmap (PackageLocationCheck cmdName) . switch $
               help "Check if running in Daml package."
            <> long "package-check" <> internal

enableScriptServiceOpt :: Parser EnableScriptService
enableScriptServiceOpt = fmap EnableScriptService $
    flagYesNoAuto "scripts" True desc idm
    where
        desc =
            "Control whether to start the Script Service, \
            \enabling/disabling support for running Daml Scripts"

studioAutorunAllScriptsOpt :: Parser StudioAutorunAllScripts
studioAutorunAllScriptsOpt =
    fmap (StudioAutorunAllScripts . determineAuto False) $
      combineFlags
        <$> flagYesNoAuto' "studio-auto-run-all-scripts" "Control whether Scripts should automatically run on opening a file in Daml Studio." idm
        <*> flagYesNoAuto' "studio-auto-run-all-scenarios" "(Deprecated) Control whether Scripts should automatically run on opening a file in Daml Studio. Always superseded by studio-auto-run-all-scenarios." internal
    where
        combineFlags Auto scenarios = scenarios
        combineFlags scripts _ = scripts -- Scripts flag always takes precedence.

enableInterfacesOpt :: Parser EnableInterfaces
enableInterfacesOpt = EnableInterfaces <$>
    flagYesNoAuto "enable-interfaces" True desc internal
    where
        desc =
            "Enable/disable support for interfaces as a language feature. \
            \If disabled, defining interfaces and interface instances is a compile-time error. \
            \On by default."

forceUtilityPackageOpt :: Parser ForceUtilityPackage
forceUtilityPackageOpt = ForceUtilityPackage <$>
    flagYesNoAuto "force-utility-package" False desc internal
    where
        desc =
            "Force a given package to compile as a utility package. \
            \This will make all data types unserializable, and will reject template/exception definitions"

explicitSerializable :: Parser ExplicitSerializable
explicitSerializable = ExplicitSerializable <$>
    flagYesNoAuto "explicit-serializable" False desc idm
    where
        desc =
            "Require explicit Serializable instances on data types in order to use them in templates and choices. \
            \Stop automatically inferring serializability of data types. \
            \This means data types used in fields of templates and choices will require explicit Serializable instances. \
            \Currently opt-in, but this will become the default in a future release."

dlintRulesFileParser :: Parser DlintRulesFile
dlintRulesFileParser =
  lastOr DefaultDlintRulesFile $
    defaultDlintRulesFile <|> explicitDlintRulesFile
  where
    defaultDlintRulesFile =
      flag' DefaultDlintRulesFile
        ( long "lint-default-rules"
          <> internal
          <> help "Use the default rules file for linting"
        )
    explicitDlintRulesFile =
      ExplicitDlintRulesFile <$> strOptionOnce
        ( long "lint-rules-file"
          <> metavar "FILE"
          <> internal
          <> help "Use FILE as the rules file for linting"
        )

dlintHintFilesParser :: Parser DlintHintFiles
dlintHintFilesParser =
  lastOr ImplicitDlintHintFile $
    implicitDlintHintFile <|> explicitDlintHintFiles <|> clearDlintHintFiles
  where
    implicitDlintHintFile =
      flag' ImplicitDlintHintFile
        ( long "lint-implicit-hint-file"
          <> internal
          <> help "Use the first '.dlint.yaml' file found in the \
                  \package directory or any parent thereof, or, failing that, \
                  \in the home directory of the current user."
        )
    explicitDlintHintFiles =
      fmap ExplicitDlintHintFiles $
        some $ Options.Applicative.strOption
          ( long "lint-hint-file"
            <> metavar "FILE"
            <> internal
            <> help "Add FILE as a hint file for linting. Any implicit \
                    \'.dlint.yaml' files will be ignored."
          )
    clearDlintHintFiles =
      flag' NoDlintHintFiles
        ( long "lint-no-hint-files"
          <> internal
          <> help "Use no hint files for linting. This also ignores any \
                  \implicit '.dlint.yaml' files"
        )

dlintOptionsParser :: Parser DlintOptions
dlintOptionsParser = DlintOptions
  <$> dlintRulesFileParser
  <*> dlintHintFilesParser

-- | Use @'disabledDlintUsageParser'@ as the @Parser DlintUsage@ argument of
-- @optionsParser@ for commands that never perform any linting.
--
-- No lint related options will appear for the user.
disabledDlintUsageParser :: Parser DlintUsage
disabledDlintUsageParser = pure DlintDisabled

-- | Use @'enabledDlintUsageParser'@ as the @Parser DlintUsage@ argument of
-- @optionsParser@ for commands that always perform linting.
--
-- The options that modify linting settings will be available, but not the ones
-- for enabling/disabling linting itself.
enabledDlintUsageParser :: Parser DlintUsage
enabledDlintUsageParser = DlintEnabled <$> dlintOptionsParser

-- | Use @'optionalDlintUsageParser' enabled@ as the @Parser DlintUsage@
-- argument of @optionsParser@ for commands where the user can decide whether
-- or not to perform linting.
--
-- @enabled@ sets the default behavior if the user doesn't explicitly enable or
-- disable linting.
--
-- The options that modify linting settings will be available, as well as
-- two options for enabling/disabling linting.
optionalDlintUsageParser :: Bool -> Parser DlintUsage
optionalDlintUsageParser def =
  fromParsed
    <$> lastOr def (enableDlint <|> disableDlint)
    <*> dlintOptionsParser
  where
    fromParsed enabled options
      | enabled = DlintEnabled options
      | otherwise = DlintDisabled

    enableDlint = flag' True
      ( long "with-dlint"
        <> internal
        <> help "Enable dlint"
      )
    disableDlint = flag' False
      ( long "without-dlint"
        <> internal
        <> help "Disable dlint"
      )

cliOptLogLevel :: Parser Logger.Priority
cliOptLogLevel =
    flag' Logger.Debug (long "debug" <> help "Set log level to DEBUG") <|>
    optionOnce readLogLevel (long "log-level" <> help "Set log level. Possible values are DEBUG, INFO, WARNING, ERROR" <> value Logger.Info)
  where
    readLogLevel = maybeReader $ \s -> case lower s of
        -- we support telemetry log-level for debugging purposes.
        "telemetry" -> Just Logger.Telemetry
        "debug" -> Just Logger.Debug
        "info" -> Just Logger.Info
        "warning" -> Just Logger.Warning
        "error" -> Just Logger.Error
        _ -> Nothing

cliOptDetailLevel :: Parser Pretty.PrettyLevel
cliOptDetailLevel =
  fmap (maybe Pretty.prettyNormal Pretty.PrettyLevel) $
    optional $ optionOnce auto $ long "detail" <> metavar "LEVEL" <> help "Detail level of the pretty printed output (default: 0)"

optPackageName :: Parser (Maybe GHC.UnitId)
optPackageName = optional $ fmap GHC.stringToUnitId $ strOptionOnce $
       metavar "PACKAGE-NAME"
    <> help "create package artifacts for the given package name"
    <> long "package-name"

-- | Parametrized by the type of pkgname parser since we want that to be different for
-- "package".
optionsParser :: Int -> EnableScriptService -> Parser (Maybe GHC.UnitId) -> Parser DlintUsage -> Parser Options
optionsParser numProcessors enableScriptService parsePkgName parseDlintUsage = do
    let parseUnitId Nothing = (Nothing, Nothing)
        parseUnitId (Just unitId) = case splitUnitId unitId of
            (name, mbVersion) -> (Just name, mbVersion)
    ~(optMbPackageName, optMbPackageVersion) <-
        fmap parseUnitId parsePkgName

    let optMbPackageConfigPath = Nothing
    optImportPath <- optImportPath
    optPackageDbs <- optPackageDir
    optAccessTokenPath <- optAccessTokenPath
    let optStablePackages = Nothing
    let optIfaceDir = Nothing
    optPackageImports <- many optPackageImport
    optShakeProfiling <- shakeProfilingOpt
    optThreads <- optShakeThreads
    optDamlLfVersion <- lfVersionOpt
    optLogLevel <- cliOptLogLevel
    optDetailLevel <- cliOptDetailLevel
    optGhcCustomOpts <- optGhcCustomOptions
    let optScriptService = enableScriptService
    let optSkipScriptValidation = SkipScriptValidation False
    optDlintUsage <- parseDlintUsage
    optIsGenerated <- optIsGenerated
    optDflagCheck <- optNoDflagCheck
    let optCoreLinting = False
    let optHaddock = Haddock False
    let optIncrementalBuild = IncrementalBuild False
    let optIgnorePackageMetadata = IgnorePackageMetadata False
    let optEnableOfInterestRule = False
    optCppPath <- optCppPath
    optEnableInterfaces <- enableInterfacesOpt
    optTestFilter <- compilePatternExpr <$> optTestPattern
    let optHideUnitId = False
    optUpgradeInfo <- optUpgradeInfo
    ~(optInlineDamlCustomWarningFlags, optTypecheckerWarningFlags, optLfConversionWarningFlags) <- optWarningFlags
    optIgnoreDataDepVisibility <- optIgnoreDataDepVisibility
    let optResolutionData = Nothing
    optForceUtilityPackage <- forceUtilityPackageOpt
    optExplicitSerializable <- explicitSerializable

    return Options{..}
  where
    optAccessTokenPath :: Parser (Maybe FilePath)
    optAccessTokenPath = optional . optionOnce str
        $ metavar "PATH"
        <> long "access-token-file"
        <> help "--access-token-file is deprecated, use DPM instead\nPath to the token-file for ledger authorization."

    optImportPath :: Parser [FilePath]
    optImportPath =
        many $
        Options.Applicative.strOption $
        metavar "INCLUDE-PATH" <>
        help "Path to an additional source directory to be included" <>
        long "include"

    optPackageDir :: Parser [FilePath]
    optPackageDir = many $ Options.Applicative.strOption $ metavar "LOC-OF-PACKAGE-DB"
                      <> help "use package database in the given location"
                      <> long "package-db"

    optPackageImport :: Parser PackageFlag
    optPackageImport =
      Options.Applicative.option readPackageImport $
      metavar "PACKAGE" <>
      help "explicit import of a package with optional renaming of modules" <>
      long "package" <>
      internal

    -- This is a slightly adapted version of GHC’s @parsePackageFlag@ from DynFlags
    -- which is sadly not exported.
    -- We use ReadP to stick as close to GHC’s implementation as possible.
    -- The only difference is that we fix it to parsing -package-id flags and
    -- therefore unit ids whereas GHC’s implementation is generic.
    --
    -- Here are a couple of examples for the syntax:
    --
    --  * @--package foo@ is @ModRenaming True []@
    --  * @--package foo ()@ is @ModRenaming False []@
    --  * @--package foo (A)@ is @ModRenaming False [("A", "A")]@
    --  * @--package foo (A as B)@ is @ModRenaming [("A", "B")]@
    --  * @--package foo with (A as B)@ is @ModRenaming True [("A", "B")]@
    readPackageImport :: ReadM PackageFlag
    readPackageImport = maybeReader $ \str ->
        case filter ((=="").snd) (R.readP_to_S (parse str) str) of
            [(r, "")] -> Just r
            _ -> Nothing
      where parse str = do
                pkg_arg <- tok GHC.parseUnitId
                let mk_expose = ExposePackage ("--package " <> str) (UnitIdArg pkg_arg)
                do _ <- tok $ R.string "with"
                   fmap (mk_expose . ModRenaming True) parseRns
                 R.<++ fmap (mk_expose . ModRenaming False) parseRns
                 R.<++ return (mk_expose ( ModRenaming True []))
            parseRns :: R.ReadP [(GHC.ModuleName, GHC.ModuleName)]
            parseRns = do
                _ <- tok $ R.char '('
                rns <- tok $ R.sepBy parseItem (tok $ R.char ',')
                _ <- tok $ R.char ')'
                return rns
            parseItem = do
                orig <- tok GHC.parseModuleName
                do _ <- tok $ R.string "as"
                   new <- tok GHC.parseModuleName
                   return (orig, new)
                 R.+++ return (orig, orig)
            tok :: R.ReadP a -> R.ReadP a
            tok m = m >>= \x -> R.skipSpaces >> return x

    optIsGenerated :: Parser Bool
    optIsGenerated =
        switch $
        help "Tell the compiler that the source was generated." <>
        long "generated-src" <>
        internal

    optTestPattern :: Parser (Maybe String)
    optTestPattern = optional . optionOnce str
        $ metavar "PATTERN"
        <> long "test-pattern"
        <> short 'p'
        <> help "Only scripts with names containing the given pattern will be executed."

    compilePatternExpr :: Maybe String -> (T.Text -> Bool)
    compilePatternExpr = \case
      Nothing -> const True
      Just needle ->  T.isInfixOf (T.pack needle)

    -- optparse-applicative does not provide a nice way
    -- to make the argument for -j optional, see
    -- https://github.com/pcapriotti/optparse-applicative/issues/243
    optShakeThreads :: Parser Int
    optShakeThreads =
        flag' numProcessors
          (short 'j' <>
           internal) <|>
        optionOnce auto
          (long "jobs" <>
           metavar "THREADS" <>
           help threadsHelp <>
           value 1)
    threadsHelp =
        unlines
            [ "The number of threads to run in parallel."
            , "When -j is not passed, 1 thread is used."
            , "If -j is passed, the number of threads defaults to the number of processors."
            , "Use --jobs=N to explicitely set the number of threads to N."
            , "Note that the output is not deterministic for > 1 job."
            ]


    optNoDflagCheck :: Parser Bool
    optNoDflagCheck =
      flag True False $
      help "Dont check generated GHC DynFlags for errors." <>
      long "no-dflags-check" <>
      internal

    optCppPath :: Parser (Maybe FilePath)
    optCppPath = optional . optionOnce str
        $ metavar "PATH"
        <> long "cpp"
        <> help "Set path to CPP."
        <> internal

    optUpgradeDar :: Parser (Maybe FilePath)
    optUpgradeDar = optional . optionOnce str
        $ metavar "UPGRADE_DAR"
        <> long "upgrades"
        <> help "Set DAR to upgrade"

    optTypecheckUpgrades :: Parser Bool
    optTypecheckUpgrades =
      flagYesNoAuto
        "typecheck-upgrades"
        defaultUiTypecheckUpgrades
        "Typecheck upgrades."
        idm

    optWarningFlags :: Parser (WarningFlags InlineDamlCustomWarnings, WarningFlags TypeCheckerError.ErrorOrWarning, WarningFlags LFConversion.ErrorOrWarning)
    optWarningFlags = unwrap . splitWarningFlags <$> runParser allWarningFlagParsers
      where
      unwrap :: Product '[a, b, c] -> (a, b, c)
      unwrap (ProdT inlineDamlCustomWarnings (ProdT typecheckerError (ProdT lfConversion ProdZ))) =
        (inlineDamlCustomWarnings, typecheckerError, lfConversion)

    optUpgradeInfo :: Parser UpgradeInfo
    optUpgradeInfo = do
      uiTypecheckUpgrades <- optTypecheckUpgrades
      uiUpgradedPackagePath <- optUpgradeDar
      pure UpgradeInfo {..}

    optIgnoreDataDepVisibility :: Parser IgnoreDataDepVisibility
    optIgnoreDataDepVisibility =
      IgnoreDataDepVisibility <$>
        flagYesNoAuto
          "ignore-data-deps-visibility"
          False
          ( "Ignore explicit exports on data-dependencies, and instead allow importing of all definitions from that package\n"
            <> "(This was the default behaviour before Daml 2.10)"
          )
          idm

optGhcCustomOptions :: Parser [String]
optGhcCustomOptions =
    fmap concat $ many $
    Options.Applicative.option (stringsSepBy ' ') $
    long "ghc-option" <>
    metavar "OPTION" <>
    help "Options to pass to the underlying GHC"

shakeProfilingOpt :: Parser (Maybe FilePath)
shakeProfilingOpt = optional $ strOptionOnce $
       metavar "PROFILING-REPORT"
    <> help "Directory for Shake profiling reports"
    <> long "shake-profiling"

incrementalBuildOpt :: Parser IncrementalBuild
incrementalBuildOpt = IncrementalBuild <$> flagYesNoAuto "incremental" False "Enable incremental builds" idm

studioReplaceOpt :: Parser ReplaceExtension
studioReplaceOpt =
    Options.Applicative.option readReplacement $
      long "replace"
        <> help "Whether an existing extension should be overwritten. ('never' or 'always' for bundled extension version, 'published' for official published version of extension, defaults to 'published')"
        <> value ReplaceExtPublished
  where
    readReplacement = maybeReader $ \arg ->
      case lower arg of
        "never" -> Just ReplaceExtNever
        "always" -> Just ReplaceExtAlways
        "published" -> Just ReplaceExtPublished
        _ -> Nothing
