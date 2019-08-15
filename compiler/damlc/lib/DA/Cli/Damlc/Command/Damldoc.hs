-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Cli.Damlc.Command.Damldoc(cmdDamlDoc) where

import DA.Cli.Options
import DA.Daml.Doc.Driver
import DA.Daml.Doc.Extract
import DA.Daml.Options
import DA.Daml.Options.Types
import Development.IDE.Types.Location
import Development.IDE.Types.Options

import Options.Applicative
import Data.List.Extra
import qualified Data.Text as T

------------------------------------------------------------

cmdDamlDoc :: Mod CommandFields (IO ())
cmdDamlDoc = command "docs" $
             info (helper <*> (exec <$> documentation)) $
             progDesc "Generate documentation for the given DAML program."
             <> fullDesc

documentation :: Parser CmdArgs
documentation = Damldoc
                <$> optInputFormat
                <*> optOutputPath
                <*> optOutputFormat
                <*> optMbPackageName
                <*> optTemplate
                <*> optOmitEmpty
                <*> optDataOnly
                <*> optNoAnnot
                <*> optInclude
                <*> optExclude
                <*> optCombine
                <*> optExtractOptions
                <*> argMainFiles
  where
    optInputFormat :: Parser InputFormat
    optInputFormat =
        option readInputFormat
            $ metavar "FORMAT"
            <> help "Input format, either daml or json (default is daml)."
            <> long "input-format"
            <> value InputDaml

    readInputFormat =
        eitherReader $ \arg ->
            case lower arg of
                "daml" -> Right InputDaml
                "json" -> Right InputJson
                _ -> Left "Unknown input format. Expected daml or json."

    optOutputPath :: Parser FilePath
    optOutputPath =
        option str
            $ metavar "OUTPUT"
            <> help "Path to output folder. If the --combine flag is passed, this is the path to the output file instead. (required)"
            <> long "output"
            <> short 'o'

    optMbPackageName :: Parser (Maybe String)
    optMbPackageName =
        optional . option str
            $ metavar "NAME"
            <> help "Name of package to generate."
            <> long "package-name"

    optTemplate :: Parser (Maybe FilePath)
    optTemplate =
        optional . option str
            $ metavar "FILE"
            <> help "Path to mustache template. The variables 'title' and 'body' in the template are substituted with the doc title and body respectively. (Exception: for hoogle and json output, the template file is a prefix to the body, no replacement occurs.)" -- TODO: make template behavior uniform accross formats
            <> long "template"
            <> short 't'

    argMainFiles :: Parser [FilePath]
    argMainFiles = some $ argument str $ metavar "FILE..."
                  <> help "Main file(s) (*.daml) whose contents are read"

    optOutputFormat :: Parser OutputFormat
    optOutputFormat =
        option readOutputFormat $
            metavar "FORMAT"
            <> help "Output format. Valid format names: rst, md, markdown, html, hoogle, json (Default: markdown)."
            <> short 'f'
            <> long "format"
            <> value (OutputDocs Markdown)

    readOutputFormat =
        eitherReader $ \arg ->
            case lower arg of
                "rst" -> Right (OutputDocs Rst)
                "md" -> Right (OutputDocs Markdown)
                "markdown" -> Right (OutputDocs Markdown)
                "html" -> Right (OutputDocs Html)
                "hoogle" -> Right OutputHoogle
                "json" -> Right OutputJson
                _ -> Left "Unknown output format. Expected rst, md, markdown, html, hoogle, or json."

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

    optCombine :: Parser Bool
    optCombine = switch $
        long "combine"
        <> help "Combine all generated docs into a single output file (always on for json and hoogle output)."

    optExtractOptions :: Parser ExtractOptions
    optExtractOptions = ExtractOptions
        <$> optQualifyTypes
        <*> optSimplifyQualifiedTypes

    optQualifyTypes :: Parser QualifyTypes
    optQualifyTypes = option readQualifyTypes $
        long "qualify-types"
        <> metavar "MODE"
        <> help
            ("Qualify any non-local types in generated docs. "
            <> "Can be set to \"always\" (always qualify non-local types), "
            <> "\"never\" (never qualify non-local types), "
            <> "and \"inpackage\" (qualify non-local types defined in the "
            <> "same package). Defaults to \"never\".")
         <> value QualifyTypesNever
         <> internal

    readQualifyTypes =
        eitherReader $ \arg ->
            case lower arg of
                "always" -> Right QualifyTypesAlways
                "inpackage" -> Right QualifyTypesInPackage
                "never" -> Right QualifyTypesNever
                _ -> Left "Unknown mode for --qualify-types. Expected \"always\", \"never\", or \"inpackage\"."

    optSimplifyQualifiedTypes :: Parser Bool
    optSimplifyQualifiedTypes = switch $
        long "simplify-qualified-types"
        <> help "Simplify qualified types by dropping the common module prefix. See --qualify-types option."
        <> internal

------------------------------------------------------------

-- Command Execution

data CmdArgs = Damldoc { cInputFormat :: InputFormat
                       , cOutputPath :: FilePath
                       , cOutputFormat :: OutputFormat
                       , cPkgName :: Maybe String
                       , cTemplate :: Maybe FilePath
                       , cOmitEmpty :: Bool
                       , cDataOnly  :: Bool
                       , cNoAnnot   :: Bool
                       , cIncludeMods :: [String]
                       , cExcludeMods :: [String]
                       , cCombine :: Bool
                       , cExtractOptions :: ExtractOptions
                       , cMainFiles :: [FilePath]
                       }
             deriving (Eq, Show, Read)

exec :: CmdArgs -> IO ()
exec Damldoc{..} = do
    opts <- fmap (\opts -> opts {optHaddock=Haddock True}) $ defaultOptionsIO Nothing
    runDamlDoc DamldocOptions
        { do_ideOptions = toCompileOpts opts { optMbPackageName = cPkgName } (IdeReportProgress False)
        , do_outputPath = cOutputPath
        , do_outputFormat = cOutputFormat
        , do_inputFormat = cInputFormat
        , do_inputFiles = map toNormalizedFilePath cMainFiles
        , do_docTemplate = cTemplate
        , do_transformOptions = transformOptions
        , do_docTitle = T.pack <$> cPkgName
        , do_combine = cCombine
        , do_extractOptions = cExtractOptions
        }

  where
    transformOptions =
        [ IncludeModules cIncludeMods | not $ null cIncludeMods] <>
        [ ExcludeModules cExcludeMods | not $ null cExcludeMods] <>
        [ DataOnly | cDataOnly ] <>
        [ IgnoreAnnotations | cNoAnnot ] <>
        [ OmitEmpty | cOmitEmpty]
