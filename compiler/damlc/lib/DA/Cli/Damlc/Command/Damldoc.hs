-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Cli.Damlc.Command.Damldoc(cmd, exec) where

import DA.Cli.Options
import DA.Daml.Compiler.Output
import DA.Daml.Doc.Driver
import DA.Daml.Doc.Extract
import DA.Daml.Options.Types
import Development.IDE.Types.Location
import Module (unitIdString)

import Options.Applicative
import Data.List.Extra
import qualified Data.Text as T
import qualified Data.Set as Set

------------------------------------------------------------

cmd :: Int -> (CmdArgs -> a) -> Mod CommandFields a
cmd numProcessors f = command "docs" $
        info (helper <*> (f <$> documentation numProcessors)) $
        progDesc "Early Access (Labs). Generate documentation for the given Daml program."
        <> fullDesc

documentation :: Int -> Parser CmdArgs
documentation numProcessors = Damldoc
    <$> optionsParser
          numProcessors
          (EnableScenarioService False)
          optPackageName
          disabledDlintUsageParser
    <*> optInputFormat
    <*> optOutputPath
    <*> optOutputFormat
    <*> optTemplate
    <*> optIndexTemplate
    <*> optHoogleTemplate
    <*> optOmitEmpty
    <*> optDataOnly
    <*> optNoAnnot
    <*> optInclude
    <*> optExclude
    <*> optExcludeInstances
    <*> optDropOrphanInstances
    <*> optCombine
    <*> optExtractOptions
    <*> optBaseURL
    <*> optHooglePath
    <*> optAnchorPath
    <*> optExternalAnchorPath
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

    optBaseURL :: Parser (Maybe T.Text)
    optBaseURL =
        optional . fmap T.pack . option str
            $ metavar "URL"
            <> help "Base URL for generated documentation."
            <> long "base-url"

    optHooglePath :: Parser (Maybe FilePath)
    optHooglePath =
        optional . option str
            $ metavar "PATH"
            <> help "Path to output hoogle database."
            <> long "output-hoogle"

    optAnchorPath :: Parser (Maybe FilePath)
    optAnchorPath =
        optional . option str
            $ metavar "PATH"
            <> help "Path to output anchor table."
            <> long "output-anchor"

    optExternalAnchorPath :: Parser (Maybe FilePath)
    optExternalAnchorPath =
        optional . option str
            $ metavar "PATH"
            <> help "Path to input anchor table (for external anchors)."
            <> long "input-anchor"

    optTemplate :: Parser (Maybe FilePath)
    optTemplate =
        optional . option str
            $ metavar "FILE"
            <> help "Path to mustache template. The variables 'title' and 'body' in the template are substituted with the doc title and body respectively. (Exception: for hoogle and json output, the template file is a prefix to the body, no replacement occurs.)" -- TODO: make template behavior uniform accross formats
            <> long "template"
            <> short 't'

    optIndexTemplate :: Parser (Maybe FilePath)
    optIndexTemplate =
        optional . option str
            $ metavar "FILE"
            <> help "Path to mustache template for index, when rendering to a folder. The variable 'body' in the template is substituted with a module index."
            <> long "index-template"

    optHoogleTemplate :: Parser (Maybe FilePath)
    optHoogleTemplate =
        optional . option str
            $ metavar "FILE"
            <> help "Path to mustache template for hoogle database."
            <> long "hoogle-template"

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
                "json" -> Right OutputJson
                _ -> Left "Unknown output format. Expected rst, md, markdown, html, or json."

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

    optExcludeInstances :: Parser (Set.Set String)
    optExcludeInstances = fmap Set.fromList . option (stringsSepBy ',') $
        metavar "NAME[,NAME...]"
        <> long "exclude-instances"
        <> help ("Exclude instances from docs by class name. " <>
                "Example: `HasField'. Default: none.")
        <> value []

    optDropOrphanInstances :: Parser Bool
    optDropOrphanInstances = switch $
        long "drop-orphan-instances"
        <> help "Drop orphan instance docs."

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

data CmdArgs = Damldoc
    { cOptions :: Options
    , cInputFormat :: InputFormat
    , cOutputPath :: FilePath
    , cOutputFormat :: OutputFormat
    , cTemplate :: Maybe FilePath
    , cIndexTemplate :: Maybe FilePath
    , cHoogleTemplate :: Maybe FilePath
    , cOmitEmpty :: Bool
    , cDataOnly  :: Bool
    , cNoAnnot   :: Bool
    , cIncludeMods :: [String]
    , cExcludeMods :: [String]
    , cExcludeInstances :: Set.Set String
    , cDropOrphanInstances :: Bool
    , cCombine :: Bool
    , cExtractOptions :: ExtractOptions
    , cBaseURL :: Maybe T.Text
    , cHooglePath :: Maybe FilePath
    , cAnchorPath :: Maybe FilePath
    , cExternalAnchorPath :: Maybe FilePath
    , cMainFiles :: [FilePath]
    }

exec :: CmdArgs -> IO ()
exec Damldoc{..} = do
    runDamlDoc DamldocOptions
        { do_compileOptions = cOptions
            { optHaddock = Haddock True
            , optScenarioService = EnableScenarioService False
            }
        , do_diagsLogger = diagnosticsLogger
        , do_outputPath = cOutputPath
        , do_outputFormat = cOutputFormat
        , do_inputFormat = cInputFormat
        , do_inputFiles = map toNormalizedFilePath' cMainFiles
        , do_docTemplate = cTemplate
        , do_docIndexTemplate = cIndexTemplate
        , do_docHoogleTemplate = cHoogleTemplate
        , do_transformOptions = transformOptions
        , do_docTitle = T.pack . unitIdString <$> optUnitId cOptions
        , do_combine = cCombine
        , do_extractOptions = cExtractOptions
        , do_baseURL = cBaseURL
        , do_hooglePath = cHooglePath
        , do_anchorPath = cAnchorPath
        , do_externalAnchorPath = cExternalAnchorPath
        }

  where
    transformOptions = TransformOptions
        { to_includeModules = if null cIncludeMods then Nothing else Just cIncludeMods
        , to_excludeModules = if null cExcludeMods then Nothing else Just cExcludeMods
        , to_excludeInstances = cExcludeInstances
        , to_dropOrphanInstances = cDropOrphanInstances
        , to_dataOnly = cDataOnly
        , to_ignoreAnnotations = cNoAnnot
        , to_omitEmpty = cOmitEmpty
        }
