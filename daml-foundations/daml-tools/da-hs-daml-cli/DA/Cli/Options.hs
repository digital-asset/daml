-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ApplicativeDo #-}
module DA.Cli.Options
  ( module DA.Cli.Options
  ) where

import qualified Data.Text           as T
import           Data.List.Extra     (trim, splitOn)
import Options.Applicative.Extended
import Data.List
import Text.Read
import qualified DA.Pretty           as Pretty
import DA.Daml.GHC.Compiler.Options
import qualified DA.Daml.LF.Ast.Version as LF
import DAML.Project.Consts
import DAML.Project.Types


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

inputFileOpt :: Parser FilePath
inputFileOpt = argument str $
       metavar "FILE"
    <> help "Input .daml file whose contents are read"

inputDarOpt :: Parser FilePath
inputDarOpt = argument str $
       metavar "FILE"
    <> help "Input .dar file whose contents are read"

inputFileRstOpt :: Parser FilePath
inputFileRstOpt = argument str $
       metavar "FILE"
    <> help "Input .rst file whose contents are read"

inputDamlPackageOpt :: Parser FilePath
inputDamlPackageOpt = argument str $
        metavar "PACKAGE_FILE"
    <> help "Input DAML Package to create the package from"

targetDirOpt :: Parser FilePath
targetDirOpt = argument str $
        metavar "TARGET_DIR"
    <> help "Target directory for DAR package"

optionalInputFileOpt :: Parser (Maybe FilePath)
optionalInputFileOpt = option (Just <$> str) $
       metavar "FILE"
    <> help "Optional input DAML file"
    <> short 'i'
    <> long "input"
    <> value Nothing

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

groupIdOpt :: Parser String
groupIdOpt = argument str $
        metavar "GROUP"
    <> help "Artifact's group Id"

artifactIdOpt :: Parser String
artifactIdOpt = argument str $
        metavar "ARTIFACT"
    <> help "Artifact's artifact Id"

versionOpt :: Parser String
versionOpt = argument str $
        metavar "VERSION"
    <> help "Artifact's version"

lfVersionOpt :: Parser LF.Version
lfVersionOpt = option (str >>= select) $
       metavar "DAML-LF-VERSION"
    <> help ("DAML-LF version to output: " ++ versionsStr)
    <> long "target"
    <> value LF.versionDefault
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

damlLfOpt :: Parser Bool
damlLfOpt = switch $
       long "daml-lf"
    <> help "Use the DAML-LF archive format"

styleOpt :: Parser Style
styleOpt = option (str >>= select) $
       metavar "STYLE"
    <> help "Pretty printing style: \"plain\", \"colored\" (default)"
    <> short 's'
    <> long "style"
    <> value Colored
  where
    select :: String -> ReadM Style
    select s = case () of
      _ | s == "plain"   -> return Plain
      _ | s == "colored" -> return Colored
      _ -> readerError $ "Unknown styleOpt '" <> show s <> "'"

junitFileOpt :: Parser (Maybe FilePath)
junitFileOpt = option (Just <$> str) $
       metavar "FILE"
    <> help "Name of the junit output xml file."
    <> long "junit"
    <> value Nothing

junitPackageNameOpt :: Parser (Maybe String)
junitPackageNameOpt = option (Just <$> str) $
       metavar "NAME"
    <> help "Name to be displayed as the JUnit package name. Path of the file will be used if none passed."
    <> long "junit-package"
    <> value Nothing

encryptOpt :: Parser Bool
encryptOpt = switch $
       long "encrypt"
    <> short 'e'
    <> help "Encrypt sdaml or not"

-- Looks quite a bit like 'damlLfOpt', but is not quite that
damlLfOutputOpt :: Parser Bool
damlLfOutputOpt = switch $
       long "daml-lf"
    <> help "Generate an output .dalf file"

dumpTypeCheckTraceOpt :: Parser Bool
dumpTypeCheckTraceOpt = switch $
       long "dump-trace"
    <> short 'd'
    <> help "Dump the type checker trace to a file"

dumpCoreOpt :: Parser Bool
dumpCoreOpt = switch $
       long "dump-core"
    <> help "Dump core intermediate representations to files"

isPackage :: Parser Bool
isPackage =
    switch
  $ help "The input file is a DAML package (dar)." <> long "dar-pkg"

-- | Option for specifing bind address (IPv4/IPv6)
addrOpt :: String -> Parser String
addrOpt defaultValue = option str $
      metavar "ADDR"
   <> help ( "The address from which the server should serve the API\
             \. (default " <> show defaultValue <> ")" )
   <> long "address"
   <> value defaultValue

-- | An option to set the port of the GUI server.
portOpt :: Int -> Parser Int
portOpt defaultValue = option (str >>= parse) $
       metavar "INT"
    <> help
        ( "Port with which the server should serve the client and the\
          \ API (default " <> show defaultValue <> ")"
        )
    <> long "port"
    <> value defaultValue
  where
    parse cs = case readMaybe cs of
      Just p  -> return p
      Nothing -> readerError $ "Invalid port '" <> cs <> "'."

mbPrefixOpt :: Parser (Maybe T.Text)
mbPrefixOpt = option (str >>= pure . Just . T.pack) $
       metavar "PREFIX"
    <> help "Optional prefix for the sandbox API, e.g. platform/api"
    <> value Nothing
    <> long "prefix"

ekgPortOpt :: Parser (Maybe Int)
ekgPortOpt = option (str >>= parse) $
       metavar "INT"
    <> help "Port which the server should serve the monitoring tool"
    <> value Nothing
    <> long "ekg"
  where
    parse cs = case readMaybe cs of
      Nothing -> readerError $ "Invalid port '" <> cs <> "'."
      p       -> pure p

verboseOpt :: Parser Bool
verboseOpt = switch $
       long "verbose"
    <> short 'v'
    <> help "Enable verbose output."

newtype Debug = Debug Bool
debugOpt :: Parser Debug
debugOpt = fmap Debug $
    switch $
       long "debug"
    <> short 'd'
    <> help "Enable debug output."

newtype Experimental = Experimental Bool
experimentalOpt :: Parser Experimental
experimentalOpt =
    fmap Experimental $
    switch $
    help "Enable experimental IDE features" <> long "experimental"

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
        help "Opt out of sending + telemetry to Digital Asset" <> long optOutS
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
