-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Driver
    ( DamldocOptions(..)
    , InputFormat(..)
    , OutputFormat(..)
    , RenderFormat(..)
    , DocOption(..)
    , damlDocDriver
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render
import DA.Daml.Doc.HaddockParse
import DA.Daml.Doc.Transform
import DA.Daml.Doc.Annotate

import Development.IDE.Types.Location
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Options

import Control.Monad.Extra
import Control.Monad.Except
import Data.Maybe
import System.IO
import System.Exit
import System.Directory
import System.FilePath

import qualified Data.Aeson                        as AE
import qualified Data.Aeson.Encode.Pretty          as AP
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text                         as T
import qualified Data.Text.Encoding                as T

data DamldocOptions = DamldocOptions
    { do_inputFormat :: InputFormat
    , do_ideOptions :: IdeOptions
    , do_outputPath :: FilePath
    , do_outputFormat :: OutputFormat
    , do_docTemplate :: Maybe FilePath
    , do_docOptions :: [DocOption]
    , do_inputFiles :: [NormalizedFilePath]
    , do_docTitle :: Maybe T.Text
    , do_combine :: Bool
    }

data InputFormat = InputJson | InputDaml
    deriving (Eq, Show, Read)

data OutputFormat = OutputJson | OutputHoogle | OutputDocs RenderFormat
    deriving (Eq, Show, Read)

-- | Run damldocs!
damlDocDriver :: DamldocOptions -> IO ()
damlDocDriver options = do
    docData <- inputDocData options
    renderDocData options (transformDocData options docData)

-- | Load doc data, either via the DAML typechecker or via JSON files.
inputDocData :: DamldocOptions -> IO [ModuleDoc]
inputDocData DamldocOptions{..} = do

    let printAndExit errMsg = do
            hPutStr stderr . unlines $
                [ unwords
                    $ "Error processing input from"
                    : map fromNormalizedFilePath do_inputFiles
                , errMsg
                ]
            exitFailure

        renderDiags = T.unpack . showDiagnosticsColored
        onErrorExit act = act >>= either (printAndExit . renderDiags) pure

    case do_inputFormat of
        InputJson -> do
            input <- mapM (BS.readFile . fromNormalizedFilePath) do_inputFiles
            let mbData = map (AE.eitherDecode . LBS.fromStrict) input
            applyAnnotations <$> concatMapM (either printAndExit pure) mbData

        InputDaml -> onErrorExit . runExceptT $
            fmap (applyTransform do_docOptions)
                (mkDocs do_ideOptions do_inputFiles)

-- | Transform doc data, applying annotations and
transformDocData :: DamldocOptions -> [ModuleDoc] -> [ModuleDoc]
transformDocData DamldocOptions {..} = applyTransform do_docOptions

-- | Output doc data.
renderDocData :: DamldocOptions -> [ModuleDoc] -> IO ()
renderDocData DamldocOptions{..} docData = do
    templateM <- mapM (fmap T.decodeUtf8 . BS.readFile) do_docTemplate

    let prefix = fromMaybe "" templateM
        write file contents = do
            createDirectoryIfMissing True $ takeDirectory file
            putStrLn $ "Writing " ++ file ++ "..."
            BS.writeFile file . T.encodeUtf8 $ prefix <> contents

    case do_outputFormat of
            OutputJson ->
                write do_outputPath $ T.decodeUtf8 . LBS.toStrict $ AP.encodePretty' jsonConf docData
            OutputHoogle ->
                write do_outputPath . T.concat $ map renderSimpleHoogle docData
            OutputDocs format -> do
                let renderOptions = RenderOptions
                        { ro_mode =
                            if do_combine
                                then RenderToFile do_outputPath
                                else RenderToFolder do_outputPath
                        , ro_format = format
                        , ro_title = do_docTitle
                        , ro_template = templateM
                        }
                renderDocs renderOptions docData
