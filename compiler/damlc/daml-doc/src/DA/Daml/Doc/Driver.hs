-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.Driver(
    damlDocDriver,
    InputFormat(..), DocFormat(..)
    , DocOption(..)
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render
import DA.Daml.Doc.HaddockParse
import DA.Daml.Doc.Transform
import DA.Daml.Doc.Annotate

import Development.IDE.Types.Location
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Options

import           Control.Monad.Extra
import           Control.Monad.Except
import qualified Data.Aeson                        as AE
import qualified Data.Aeson.Encode.Pretty          as AP
import qualified Data.ByteString.Lazy              as BS
import qualified Data.Text                         as T
import qualified Data.Text.Encoding                as T
import           System.Directory
import           System.FilePath


data InputFormat = InputJson | InputDaml
    deriving (Eq, Show, Read)

damlDocDriver :: InputFormat
              -> IdeOptions
              -> FilePath
              -> DocFormat
              -> Maybe FilePath
              -> [DocOption]
              -> [NormalizedFilePath]
              -> IO ()
damlDocDriver cInputFormat ideOpts output cFormat prefixFile options files = do

    let printAndExit errMsg = do
            putStrLn $ "Error processing input from " <> unwords (map fromNormalizedFilePath files) <> "\n"
                     <> errMsg
            fail "Aborted."

    let onErrorExit act =
            act >>= either (printAndExit . renderDiags) pure
        renderDiags = T.unpack . showDiagnosticsColored

    docData <- case cInputFormat of
            InputJson -> do
                input <- mapM (BS.readFile . fromNormalizedFilePath) files
                let mbData = map AE.eitherDecode input :: [Either String [ModuleDoc]]
                applyAnnotations <$> concatMapM (either printAndExit pure) mbData

            InputDaml ->
                onErrorExit $ runExceptT
                            $ fmap (applyTransform options)
                            $ mkDocs ideOpts files

    prefix <- maybe (pure "") BS.readFile prefixFile

    let write file contents = do
            createDirectoryIfMissing True $ takeDirectory file
            putStrLn $ "Writing " ++ file ++ "..."
            BS.writeFile file $ prefix <> BS.fromStrict (T.encodeUtf8 contents)

    case cFormat of
            Json -> write output $ T.decodeUtf8 . BS.toStrict $ AP.encodePretty' jsonConf docData
            Rst  -> write output $ renderFinish $ mconcat $ map renderSimpleRst docData
            Hoogle   -> write output $ T.concat $ map renderSimpleHoogle docData
            Markdown -> write output $ T.concat $ map renderSimpleMD docData
            Html -> sequence_
                [ write (output </> hyphenated (unModulename md_name) <> ".html") $ renderSimpleHtml m
                | m@ModuleDoc{..} <- docData ]
                    where hyphenated = T.unpack . T.replace "." "-"
    putStrLn "Done"
