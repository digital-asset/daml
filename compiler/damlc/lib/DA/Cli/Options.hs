-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ApplicativeDo #-}
module DA.Cli.Options
  ( module DA.Cli.Options
  ) where

import Data.Bifunctor
import           Data.List.Extra     (trim, splitOn)
import Options.Applicative.Extended
import Safe (lastMay)
import Data.List
import Data.Maybe
import qualified DA.Pretty           as Pretty
import DA.Daml.Options.Types
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import qualified Module as GHC
import Text.Read


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
    option (Just <$> str) $
    metavar "TARGET_SRC_DIR"
    <> help "Optional target directory to write created sources to"
    <> long "srcdir"
    <> value Nothing

qualOpt :: Parser (Maybe String)
qualOpt =
    option (Just <$> str) $
    metavar "QUALIFICATION" <>
    help "Optional qualification to append to generated module name." <>
    long "qualify" <>
    value Nothing

outputFileOpt :: Parser String
outputFileOpt = strOption $
       metavar "FILE"
    <> help "Output file (use '-' for stdout)"
    <> short 'o'
    <> long "output"
    <> value "-"

optionalOutputFileOpt :: Parser (Maybe String)
optionalOutputFileOpt = option (Just <$> str) $
       metavar "FILE"
    <> help "Optional output file (defaults to <PACKAGE-NAME>.dar)"
    <> short 'o'
    <> long "output"
    <> value Nothing

targetFileNameOpt :: Parser (Maybe String)
targetFileNameOpt = option (Just <$> str) $
        metavar "DAR_NAME"
        <> help "Target file name of DAR package"
        <> long "dar-name"
        <> value Nothing

packageNameOpt :: Parser String
packageNameOpt = argument str $
       metavar "PACKAGE-NAME"
    <> help "Name of the DAML package"

lfVersionOpt :: Parser LF.Version
lfVersionOpt = option (str >>= select) $
       metavar "DAML-LF-VERSION"
    <> help ("DAML-LF version to output: " ++ versionsStr)
    <> long "target"
    <> value LF.versionDefault
    <> internal
  where
    renderVersion v =
      let def = if v == LF.versionDefault then " (default)" else ""
      in Pretty.renderPretty v ++ def
    versionsStr = intercalate ", " (map renderVersion LF.supportedOutputVersions)
    select = \case
      versionStr
        | Just version <- LF.parseVersion versionStr
        , version `elem` LF.supportedOutputVersions
        -> return version
        | otherwise
        -> readerError $ "Unknown DAML-LF version: " ++ versionsStr

dotFileOpt :: Parser (Maybe FilePath)
dotFileOpt = option (Just <$> str) $
       metavar "FILE"
    <> help "Name of the dot file to be generated."
    <> long "dot"
    <> value Nothing

htmlOutFile :: Parser FilePath
htmlOutFile = strOption $
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

data Telemetry = OptedIn | OptedOut | Undecided
telemetryOpt :: Parser Telemetry
telemetryOpt = do
    let optInS = "telemetry"
        optOutS = "optOutTelemetry"
    optIn <-
        switch $
        help "Send crash data + telemetry to Digital Asset" <> long optInS
    optOut <-
        switch $
        help "Opt out of sending crash data + telemetry to Digital Asset" <> long optOutS
    pure $ case (optIn, optOut) of
        (False, False) -> Undecided
        (True, False) -> OptedIn
        (False, True) -> OptedOut
        (True, True) ->
            error $
            "Both --"++optInS++" and --"++optOutS++" have been selected, you either have to opt into telemetry or opt out"

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

data ProjectOpts = ProjectOpts
    { projectRoot :: Maybe ProjectPath
    -- ^ An explicit project path specified by the user.
    , projectCheck :: ProjectCheck
    -- ^ Throw an error if this is not run in a project.
    }

projectOpts :: String -> Parser ProjectOpts
projectOpts name = ProjectOpts <$> projectRootOpt <*> projectCheckOpt name
    where
        projectRootOpt :: Parser (Maybe ProjectPath)
        projectRootOpt =
            optional $
            fmap ProjectPath $
            strOption $
            long "project-root" <>
            help
                (mconcat
                     [ "Path to the root of a project containing daml.yaml. "
                     , "If unspecified this will use the DAML_PROJECT environment variable set by the assistant."
                     ])
        projectCheckOpt cmdName = fmap (ProjectCheck cmdName) . switch $
               help "Check if running in DAML project."
            <> long "project-check"

enableScenarioOpt :: Parser EnableScenarioService
enableScenarioOpt = EnableScenarioService <$>
    flagYesNoAuto "scenarios" True "Enable/disable support for running scenarios" idm

dlintEnabledOpt :: Parser DlintUsage
dlintEnabledOpt = DlintEnabled
  <$> strOption
  ( long "with-dlint"
    <> metavar "DIR"
    <> help "Enable linting with 'dlint.yaml' directory"
  )
  <*> switch
  ( long "allow-overrides"
    <> help "Allow '.dlint.yaml' configuration overrides"
  )

dlintDisabledOpt :: Parser DlintUsage
dlintDisabledOpt = flag' DlintDisabled
  ( long "without-dlint"
    <> help "Disable dlint"
  )

dlintUsageOpt :: Parser DlintUsage
dlintUsageOpt = fmap (fromMaybe DlintDisabled . lastMay) $
  many (dlintEnabledOpt <|> dlintDisabledOpt)


optDebugLog :: Parser Bool
optDebugLog = switch $ help "Enable debug output" <> long "debug"

optPackageName :: Parser (Maybe String)
optPackageName = optional $ strOption $
       metavar "PACKAGE-NAME"
    <> help "create package artifacts for the given package name"
    <> long "package-name"

-- | Parametrized by the type of pkgname parser since we want that to be different for
-- "package".
optionsParser :: Int -> EnableScenarioService -> Parser (Maybe String) -> Parser Options
optionsParser numProcessors enableScenarioService parsePkgName = Options
    <$> optImportPath
    <*> optPackageDir
    <*> pure Nothing
    <*> parsePkgName
    <*> pure Nothing
    <*> optHideAllPackages
    <*> many optPackageImport
    <*> shakeProfilingOpt
    <*> optShakeThreads
    <*> lfVersionOpt
    <*> optDebugLog
    <*> optGhcCustomOptions
    <*> pure enableScenarioService
    <*> pure (optSkipScenarioValidation $ defaultOptions Nothing)
    <*> dlintUsageOpt
    <*> optIsGenerated
    <*> optNoDflagCheck
    <*> pure False
    <*> pure (Haddock False)
    <*> optCppPath
    <*> pure Nothing
    <*> pure (IncrementalBuild False)
  where
    optImportPath :: Parser [FilePath]
    optImportPath =
        many $
        strOption $
        metavar "INCLUDE-PATH" <>
        help "Path to an additional source directory to be included" <>
        long "include"

    optPackageDir :: Parser [FilePath]
    optPackageDir = many $ strOption $ metavar "LOC-OF-PACKAGE-DB"
                      <> help "use package database in the given location"
                      <> long "package-db"

    optPackageImport :: Parser PackageImport
    optPackageImport =
      option readPackageImport $
      metavar "PACKAGE" <>
      help "explicit import of a package with optional renaming of modules" <>
      long "package" <>
      internal

    readPackageImport = maybeReader $ \s -> do
        (unitId, exposeImplicit, modRenamings) <- readMaybe s
        pure PackageImport
          { pkgImportUnitId = GHC.stringToUnitId unitId
          , pkgImportExposeImplicit = exposeImplicit
          , pkgImportModRenamings = map (bimap GHC.mkModuleName GHC.mkModuleName) modRenamings
          }

    optHideAllPackages :: Parser Bool
    optHideAllPackages =
      switch $
      help "hide all packages, use -package for explicit import" <>
      long "hide-all-packages" <>
      internal

    optIsGenerated :: Parser Bool
    optIsGenerated =
        switch $
        help "Tell the compiler that the source was generated." <>
        long "generated-src" <>
        internal

    -- optparse-applicative does not provide a nice way
    -- to make the argument for -j optional, see
    -- https://github.com/pcapriotti/optparse-applicative/issues/243
    optShakeThreads :: Parser Int
    optShakeThreads =
        flag' numProcessors
          (short 'j' <>
           internal) <|>
        option auto
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
    optCppPath = optional . option str
        $ metavar "PATH"
        <> long "cpp"
        <> help "Set path to CPP."
        <> internal

optGhcCustomOptions :: Parser [String]
optGhcCustomOptions =
    fmap concat $ many $
    option (stringsSepBy ' ') $
    long "ghc-option" <>
    metavar "OPTION" <>
    help "Options to pass to the underlying GHC"

shakeProfilingOpt :: Parser (Maybe FilePath)
shakeProfilingOpt = optional $ strOption $
       metavar "PROFILING-REPORT"
    <> help "Directory for Shake profiling reports"
    <> long "shake-profiling"

incrementalBuildOpt :: Parser IncrementalBuild
incrementalBuildOpt = IncrementalBuild <$> flagYesNoAuto "incremental" False "Enable incremental builds" idm
