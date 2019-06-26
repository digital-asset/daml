-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings   #-}

module DA.Cli.Damlc.Command.Damldoc(cmdDamlDoc, cmdRenderDoc) where

import           DA.Cli.Damlc.Base(Command)
import           DA.Cli.Options
import           DA.Daml.GHC.Damldoc.Driver
import Development.IDE.Types.Location

import           Options.Applicative
import Data.Maybe

------------------------------------------------------------

cmdDamlDoc :: Mod CommandFields Command
cmdDamlDoc = command "docs" $
             info (helper <*> (exec <$> documentation InputDaml)) $
             progDesc "Generate documentation for the given DAML program."
             <> fullDesc


cmdRenderDoc :: Mod CommandFields Command
cmdRenderDoc = command "render-doc-json" $
               info (helper <*> (exec <$> documentation InputJson)) $
               progDesc "Render documentation data from the given json file."
               <> fullDesc


documentation :: InputFormat -> Parser CmdArgs
documentation x = Damldoc x <$>
                optOutput
                <*> optJsonOrFormat
                <*> optPrefix
                <*> optOmitEmpty
                <*> optDataOnly
                <*> optNoAnnot
                <*> optInclude
                <*> optExclude
                <*> argMainFiles
  where
    optOutput :: Parser FilePath
    optOutput = option str $ metavar "OUTPUT"
                <> help "Output name of generated files (required)"
                <> long "output"
                <> short 'o'

    optPrefix :: Parser (Maybe FilePath)
    optPrefix = option (Just <$> str) $ metavar "FILE"
                <> help "File to prepend to all generated files"
                <> long "prefix"
                <> short 'p'
                <> value Nothing

    argMainFiles :: Parser [FilePath]
    argMainFiles = some $ argument str $ metavar "FILE..."
                  <> help "Main file(s) (*.daml) whose contents are read"

    optJsonOrFormat :: Parser DocFormat
    optJsonOrFormat = fromMaybe <$>
                      optFormat <*>
                      (flag Nothing (Just Json) $
                        long "json"
                        <> help "alias for `--format Json'")

    optFormat :: Parser DocFormat
    optFormat = option auto $ metavar "FORMAT"
                <> help ("Output format. Valid format names: "
                         <> show [minBound..maxBound::DocFormat]
                         <> " (Default: Markdown).")
                <> short 'f'
                <> long "format"
                <> value Markdown

    optOmitEmpty :: Parser Bool
    optOmitEmpty = switch
                   (long "omit-empty"
                   <> help "Omit items that have no documentation at all")

    optDataOnly :: Parser Bool
    optDataOnly = switch $
                   long "data-only"
                   <> help ("Only generate documentation for templates and data "
                            <> "types (not functions and classes)")

    optNoAnnot :: Parser Bool
    optNoAnnot = switch $
                   long "ignore-annotations"
                   <> help "Ignore MOVE and HIDE annotations in the source"

    optInclude :: Parser [String]
    optInclude = option (stringsSepBy ',') $
                 metavar "PATTERN[,PATTERN...]"
                 <> long "include-modules"
                 <> help ("Include modules matching one of the given pattern. " <>
                         "Example: `DA.**.Iou_*'. Default: all.")
                 <> value []

    optExclude :: Parser [String]
    optExclude = option (stringsSepBy ',') $
                 metavar "PATTERN[,PATTERN...]"
                 <> long "exclude-modules"
                 <> help ("Skip modules matching one of the given pattern. " <>
                         "Example: `DA.**.Internal'. Default: none.")
                 <> value []

------------------------------------------------------------

-- Command Execution

data CmdArgs = Damldoc { cInputFormat :: InputFormat
                       , cOutput   :: FilePath
                       , cFormat   :: DocFormat
                       , cPrefix   :: Maybe FilePath
                       , cOmitEmpty :: Bool
                       , cDataOnly  :: Bool
                       , cNoAnnot   :: Bool
                       , cIncludeMods :: [String]
                       , cExcludeMods :: [String]
                       , cMainFiles :: [FilePath]
                       }
             deriving (Eq, Show, Read)

exec :: CmdArgs -> Command
exec Damldoc{..} = damlDocDriver cInputFormat cOutput cFormat cPrefix options (map toNormalizedFilePath cMainFiles)
  where options =
          [ IncludeModules cIncludeMods | not $ null cIncludeMods] <>
          [ ExcludeModules cExcludeMods | not $ null cExcludeMods] <>
          [ DataOnly | cDataOnly ] <>
          [ IgnoreAnnotations | cNoAnnot ] <>
          [ OmitEmpty | cOmitEmpty]
