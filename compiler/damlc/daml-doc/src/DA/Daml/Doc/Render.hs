-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Render
  ( RenderFormat(..)
  , RenderOptions(..)
  , RenderMode(..)
  , renderDocs
  , renderPage
  , renderRst
  , renderMd
  , renderModule
  , renderSimpleHoogle
  , jsonConf
  ) where

import DA.Daml.Doc.Render.Types
import DA.Daml.Doc.Render.Monoid
import DA.Daml.Doc.Render.Rst
import DA.Daml.Doc.Render.Markdown
import DA.Daml.Doc.Render.Hoogle
import DA.Daml.Doc.Render.Output
import DA.Daml.Doc.Types

import Data.Maybe
import Data.List.Extra
import Data.Foldable
import System.Directory
import System.FilePath
import System.IO
import System.Exit

import qualified CMarkGFM as GFM
import qualified Data.Aeson.Types as A
import qualified Data.Aeson.Encode.Pretty as AP
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Encoding as T
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as Map
import qualified Text.Mustache as M

-- | centralised JSON configuration for pretty-printing
jsonConf :: AP.Config
jsonConf = AP.Config (AP.Spaces 2) (AP.keyOrder ["id"]) AP.Generic True

renderDocs :: RenderOptions -> [ModuleDoc] -> IO ()
renderDocs RenderOptions{..} mods = do
    let (formatter, postProcessing) =
            case ro_format of
                Rst -> (renderRst, id)
                Markdown -> (renderMd, id)
                Html -> (renderMd, GFM.commonmarkToHtml [GFM.optUnsafe] [GFM.extTable])
        templateText = fromMaybe (defaultTemplate ro_format) ro_template

    template <- compileTemplate templateText

    case ro_mode of
        RenderToFile path -> do
            BS.writeFile path
                . T.encodeUtf8
                . renderTemplate template
                    (fromMaybe "Package Docs" ro_title)
                . postProcessing
                . renderPage formatter
                $ mconcatMap renderModule mods

        RenderToFolder path -> do
            let renderMap = Map.fromList
                    [(md_name mod, renderModule mod) | mod <- mods]
                outputMap = renderFolder formatter renderMap
                extension =
                    case ro_format of
                        Markdown -> "md"
                        Rst -> "rst"
                        Html -> "html"

                outputPath mod = path </> moduleNameToFileName mod <.> extension
                pageTitle mod = T.concat
                    [ maybe "" (<> " - ") ro_title
                    , "Module "
                    , unModulename mod ]

            createDirectoryIfMissing True path
            for_ (Map.toList outputMap) $ \ (mod, renderedOutput) -> do
                BS.writeFile (outputPath mod)
                    . T.encodeUtf8
                    . renderTemplate template (pageTitle mod)
                    . postProcessing
                    $ renderedOutput

compileTemplate :: T.Text -> IO M.Template
compileTemplate templateText =
    case M.compileMustacheText "daml docs template" templateText of
        Right t -> pure t
        Left e -> do
            hPutStrLn stderr ("Error with daml docs template: " <> show e)
            exitFailure

renderTemplate ::
    M.Template -- ^ template
    -> T.Text -- ^ page title
    -> T.Text -- ^ page body
    -> T.Text
renderTemplate template pageTitle pageBody =
    TL.toStrict . M.renderMustache template . A.object $
        [ "title" A..= pageTitle
        , "body" A..= pageBody
        ]

defaultTemplate :: RenderFormat -> T.Text
defaultTemplate = \case
    Html -> defaultTemplateHtml
    _ -> "{{{body}}}"

defaultTemplateHtml :: T.Text
defaultTemplateHtml = T.unlines
    [ "<html>"
    , "<head><title>{{title}}</title><meta charset=\"utf-8\"></head>"
    , "<body>{{{body}}}</body>"
    , "</html>"
    ]
