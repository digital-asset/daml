-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform.Instances
    ( distributeInstanceDocs
    ) where

import DA.Daml.Doc.Transform.Options
import DA.Daml.Doc.Types
import Data.Map qualified as Map
import Data.Maybe (listToMaybe)
import Data.Set qualified as Set
import Data.Text qualified as T

type InstanceMap = Map.Map Anchor (Set.Set InstanceDoc)

-- | Add relevant instances to every type and class.
distributeInstanceDocs :: TransformOptions -> [ModuleDoc] -> [ModuleDoc]
distributeInstanceDocs opts docs =
    let instanceMap = getInstanceMap docs
    in map (addInstances instanceMap) docs
  where
    filterInstanceMap :: InstanceMap -> InstanceMap
    filterInstanceMap = Map.mapMaybe $ nonEmpty . Set.filter (keepInstance opts)

    nonEmpty :: Set.Set a -> Maybe (Set.Set a)
    nonEmpty xs = if null xs then Nothing else Just xs

    getInstanceMap :: [ModuleDoc] -> InstanceMap
    getInstanceMap docs =
        Map.unionsWith Set.union (map getModuleInstanceMap docs)

    getModuleInstanceMap :: ModuleDoc -> InstanceMap
    getModuleInstanceMap ModuleDoc{..}
        = Map.unionsWith Set.union
        . map getInstanceInstanceMap
        $ md_instances

    getInstanceInstanceMap :: InstanceDoc -> InstanceMap
    getInstanceInstanceMap inst = Map.fromList
        [ (anchor, Set.singleton inst)
        | anchor <- Set.toList . getTypeAnchors $ id_type inst ]

    -- | Get the set of internal references i.e. anchors in the type expression.
    getTypeAnchors :: Type -> Set.Set Anchor
    getTypeAnchors = \case
        TypeApp (Just (Reference Nothing anchor)) _ args -> Set.unions
            $ Set.singleton anchor
            : map getTypeAnchors args
        TypeApp _ _ args -> Set.unions $ map getTypeAnchors args
        TypeFun parts -> Set.unions $ map getTypeAnchors parts
        TypeTuple parts -> Set.unions $ map getTypeAnchors parts
        TypeList p -> getTypeAnchors p
        TypeLit _ -> Set.empty

    addInstances :: InstanceMap -> ModuleDoc -> ModuleDoc
    addInstances imap ModuleDoc{..} = ModuleDoc
        { md_name = md_name
        , md_anchor = md_anchor
        , md_descr = md_descr
        , md_functions = md_functions
        , md_templates = md_templates
        , md_interfaces = map (addIfaceInstances imap) md_interfaces
        , md_classes = map (addClassInstances filteredIMap) md_classes
        , md_adts = map (addTypeInstances filteredIMap) md_adts
        , md_instances =
            if to_dropOrphanInstances opts
                then filter (not . id_isOrphan) md_instances
                else md_instances
        }
      where
        filteredIMap = filterInstanceMap imap

    addClassInstances :: InstanceMap -> ClassDoc -> ClassDoc
    addClassInstances imap cl = cl
        { cl_instances = Set.toList <$> do
            anchor <- cl_anchor cl
            Map.lookup anchor imap
        }

    addTypeInstances :: InstanceMap -> ADTDoc -> ADTDoc
    addTypeInstances imap ad = ad
        { ad_instances = Set.toList <$> do
            anchor <- ad_anchor ad
            Map.lookup anchor imap
        }

    addIfaceInstances :: InstanceMap -> InterfaceDoc -> InterfaceDoc
    addIfaceInstances imap idoc = idoc
      { if_methods =
          [ MethodDoc{..}
          | InstanceDoc {id_type, id_module, id_descr} <-
                maybe [] Set.toList $ do
                  anchor <- if_anchor idoc
                  Map.lookup anchor imap
          , Just "HasMethod" == getTypeAppName id_type
          , "DA.Internal.Desugar" == id_module
          , Just [_if_name, TypeLit name, mtd_type] <- [getTypeAppArgs id_type]
          , let mtd_name = Typename $ T.dropEnd 1 $ T.drop 1 name -- drop enclosing double-quotes.
          , let mtd_descr = id_descr
          ]
      , if_viewtype = listToMaybe
          [ InterfaceViewtypeDoc viewtype
          | InstanceDoc {id_type, id_module} <-
              maybe [] Set.toList $ do
                anchor <- if_anchor idoc
                Map.lookup anchor imap
          , Just "HasInterfaceView" == getTypeAppName id_type
          , "DA.Internal.Interface" == id_module
          , Just [_if_name, viewtype] <- [getTypeAppArgs id_type]
          ]
      }
