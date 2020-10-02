-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Extract.Templates
    ( getTemplateDocs
    , getTemplateData
    , getInstanceDocs
    , stripInstanceSuffix
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Extract.Types
import DA.Daml.Doc.Extract.Util
import DA.Daml.Doc.Extract.TypeExpr

import qualified Data.Map.Strict as MS
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Tuple.Extra (second)
import qualified Data.Set as Set
import qualified Data.Text as T

import "ghc-lib" GHC
import "ghc-lib-parser" Var (varType)
import "ghc-lib-parser" CoreSyn (isOrphan)
import "ghc-lib-parser" InstEnv
import "ghc-lib-parser" OccName

-- | Build template docs up from ADT and class docs.
getTemplateDocs ::
    DocCtx
    -> MS.Map Typename ADTDoc -- ^ maps template names to their ADT docs
    -> MS.Map Typename ClassDoc -- ^ maps template names to their template instance class docs
    -> [TemplateDoc]
getTemplateDocs DocCtx{..} typeMap templateInstanceMap =
    map mkTemplateDoc $ Set.toList dc_templates
  where
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkTemplateDoc :: Typename -> TemplateDoc
    mkTemplateDoc name = TemplateDoc
      { td_anchor = ad_anchor tmplADT
      , td_name = ad_name tmplADT
      , td_args = ad_args tmplADT
      , td_super = cl_super =<< MS.lookup name templateInstanceMap
      , td_descr = ad_descr tmplADT
      , td_payload = getFields tmplADT
      -- assumes exactly one record constructor (syntactic, template syntax)
      , td_choices = map mkChoiceDoc choices
      }
      where
        tmplADT = asADT name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices

    mkChoiceDoc :: Typename -> ChoiceDoc
    mkChoiceDoc name = ChoiceDoc
      { cd_name = ad_name choiceADT
      , cd_descr = ad_descr choiceADT
      -- assumes exactly one constructor (syntactic in the template syntax), or
      -- uses a dummy value otherwise.
      , cd_fields = getFields choiceADT
      }
          where choiceADT = asADT name

    asADT n = fromMaybe dummyDT $
              MS.lookup n typeMap
    -- returns a dummy ADT if the choice argument is not in the local type map
    -- (possible if choice instances are defined directly outside the template).
    -- This wouldn't be necessary if we used the type-checked AST.
      where dummyDT = ADTDoc { ad_anchor = Nothing
                             , ad_name = dummyName n
                             , ad_descr = Nothing
                             , ad_args = []
                             , ad_constrs = []
                             , ad_instances = Nothing
                             }

            dummyName (Typename "Archive") = Typename "Archive"
            dummyName (Typename t) = Typename $ "External:" <> t

    -- Assuming one constructor (record or prefix), extract the fields, if any.
    -- For choices without arguments, GHC returns a prefix constructor, so we
    -- need to cater for this case specially.
    getFields adt = case ad_constrs adt of
                      [PrefixC{}] -> []
                      [RecordC{ ac_fields = fields }] -> fields
                      [] -> [] -- catching the dummy case here, see above
                      _other -> error "getFields: found multiple constructors"


-- | Extracts all names of templates defined in a module,
-- and a map of template names to its set of choices
getTemplateData :: ParsedModule ->
    ( Set.Set Typename
    , MS.Map Typename (Set.Set Typename) )
getTemplateData ParsedModule{..} =
  let
    instDs    = mapMaybe (isInstDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    templates = mapMaybe isTemplate instDs
    choiceMap = MS.fromListWith (<>) $
                map (second Set.singleton) $
                mapMaybe isChoice instDs
  in
    (Set.fromList templates, choiceMap)
    where
      isInstDecl (InstD _ (ClsInstD _ i)) = Just i
      isInstDecl _ = Nothing


-- | If the given instance declaration is declaring a template instance, return
--   its name (IdP). Used to build the set of templates declared in a module.
isTemplate :: ClsInstDecl GhcPs -> Maybe Typename
isTemplate (XClsInstDecl _) = Nothing
isTemplate ClsInstDecl{..}
  | L _ ty <- getLHsInstDeclHead cid_poly_ty
  , HsAppTy _ (L _ t1) t2 <- ty
  , HsTyVar _ _ (L _ tmplClass) <- t1
  , Just (L _ tmplName) <- hsTyGetAppHead_maybe t2
  , Qual classModule classOcc <- tmplClass
  , moduleNameString classModule == "DA.Internal.Desugar"
  , occNameString classOcc == "HasCreate"
  = Just (Typename . packRdrName $ tmplName)

  | otherwise = Nothing

-- | If the given instance declaration is declaring a template choice instance,
--   return template and choice name (IdP). Used to build the set of choices
--   per template declared in a module.
isChoice :: ClsInstDecl GhcPs -> Maybe (Typename, Typename)
isChoice (XClsInstDecl _) = Nothing
isChoice ClsInstDecl{..}
  | L _ ty <- getLHsInstDeclHead cid_poly_ty
  , HsAppTy _ (L _ cApp1) (L _ _cArgs) <- ty
  , HsAppTy _ (L _ cApp2) cName <- cApp1
  , HsAppTy _ (L _ choice) cTmpl <- cApp2
  , HsTyVar _ _ (L _ choiceClass) <- choice
  , Just (L _ choiceName) <- hsTyGetAppHead_maybe cName
  , Just (L _ tmplName) <- hsTyGetAppHead_maybe cTmpl
  , Qual classModule classOcc <- choiceClass
  , moduleNameString classModule == "DA.Internal.Desugar"
  , occNameString classOcc == "HasExercise"
  = Just (Typename . packRdrName $ tmplName, Typename . packRdrName $ choiceName)

  | otherwise = Nothing

-- | Strip the @Instance@ suffix off of a typename, if it's there.
-- Otherwise returns 'Nothing'.
stripInstanceSuffix :: Typename -> Maybe Typename
stripInstanceSuffix (Typename t) = Typename <$> T.stripSuffix "Instance" t

-- | Get (normal) typeclass instances data. TODO: Correlate with
-- instance declarations via SrcSpan (like Haddock).
getInstanceDocs :: DocCtx -> ClsInst -> InstanceDoc
getInstanceDocs ctx ClsInst{..} =
    let ty = varType is_dfun
    in InstanceDoc
        { id_context = typeToContext ctx ty
        , id_type = typeToType ctx ty
        , id_isOrphan = isOrphan is_orphan
        }
