-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform.Instances
    ( distributeInstanceDocs
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Transform.Options

import qualified Data.Map as Map
import qualified Data.Set as Set

type InstanceMap = Map.Map Anchor (Set.Set InstanceDoc)

-- | Add relevant instances to every type and class.
distributeInstanceDocs :: TransformOptions -> [ModuleDoc] -> [ModuleDoc]
distributeInstanceDocs opts docs =
    let instanceMap = getInstanceMap docs
    in map (addInstances instanceMap) docs
  where
    getInstanceMap :: [ModuleDoc] -> InstanceMap
    getInstanceMap docs =
        Map.unionsWith Set.union (map getModuleInstanceMap docs)

    getModuleInstanceMap :: ModuleDoc -> InstanceMap
    getModuleInstanceMap ModuleDoc{..}
        = Map.unionsWith Set.union
        . map getInstanceInstanceMap
        . filter (keepInstance opts)
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
        , md_classes = map (addClassInstances imap) md_classes
        , md_adts = map (addTypeInstances imap) md_adts
        , md_instances =
            if to_dropOrphanInstances opts
                then filter (not . id_isOrphan) md_instances
                else md_instances
        }

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
