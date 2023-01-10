-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Extract.Templates
    ( getTemplateDocs
    , getTemplateData
    , getInstanceDocs
    , getInterfaceDocs
    ) where

import DA.Daml.Doc.Types as DDoc
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
import "ghc-lib-parser" Id

-- | Build template docs up from ADT and class docs.
getTemplateDocs ::
    DocCtx
    -> MS.Map Typename ADTDoc
      -- ^ maps type names to their ADT docs
    -> MS.Map Typename (Set.Set InterfaceInstanceDoc)
      -- ^ maps type names to the interface instances contained in their declaration.
    -> [TemplateDoc]
getTemplateDocs DocCtx{..} typeMap interfaceInstanceMap =
    map mkTemplateDoc $ Set.toList dc_templates
  where
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkTemplateDoc :: Typename -> TemplateDoc
    mkTemplateDoc name = TemplateDoc
      { td_anchor = ad_anchor tmplADT
      , td_name = ad_name tmplADT
      , td_descr = ad_descr tmplADT
      , td_payload = getFields tmplADT
      -- assumes exactly one record constructor (syntactic, template syntax)
      , td_choices = map (mkChoiceDoc typeMap) choices
      , td_interfaceInstances =
          Set.toList (MS.findWithDefault mempty name interfaceInstanceMap)
     }
      where
        tmplADT = asADT typeMap name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices


-- | Build interface docs up from class docs.
getInterfaceDocs :: DocCtx
    -> MS.Map Typename ADTDoc
        -- ^ maps type names to their ADT docs
    -> MS.Map Typename (Set.Set InterfaceInstanceDoc)
        -- ^ maps type names to the interface instances contained in their declaration.
    -> [InterfaceDoc]
getInterfaceDocs DocCtx{..} typeMap interfaceInstanceMap =
    map mkInterfaceDoc $ Set.toList dc_interfaces
  where
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkInterfaceDoc :: Typename -> InterfaceDoc
    mkInterfaceDoc name = InterfaceDoc
      { if_anchor = ad_anchor ifADT
      , if_name = ad_name ifADT
      , if_descr = ad_descr ifADT
      , if_choices = map (mkChoiceDoc typeMap) choices
      , if_methods = [] -- filled by distributeInstanceDocs
      , if_interfaceInstances =
          Set.toList (MS.findWithDefault mempty name interfaceInstanceMap)
      , if_viewtype = Nothing -- filled by distributeInstanceDocs
      }
      where
        ifADT = asADT typeMap name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices

-- | Extracts all names of templates and interfaces defined in a module,
-- and a map of template/interface names to its set of choices
getTemplateData :: ParsedModule ->
    ( Set.Set Typename
    , Set.Set Typename
    , MS.Map Typename (Set.Set Typename) )
getTemplateData ParsedModule{..} =
  let
    dataDs    = mapMaybe (isDataDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    instDs    = mapMaybe (isInstDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    templates = mapMaybe isTemplate dataDs
    interfaces = mapMaybe isInterface dataDs
    choiceMap = MS.fromListWith (<>) $
                map (second Set.singleton) $
                mapMaybe isChoice instDs
  in
    (Set.fromList templates, Set.fromList interfaces, choiceMap)
    where
      isInstDecl (InstD _ (ClsInstD _ i)) = Just i
      isInstDecl _ = Nothing
      isDataDecl (TyClD _ d@DataDecl{}) = Just d
      isDataDecl _ = Nothing


-- | If the given instance declaration is declaring a template instance, return
--   its name (IdP). Used to build the set of templates declared in a module.
isTemplate :: TyClDecl GhcPs -> Maybe Typename
isTemplate = hasGhcTypesConstraint "DamlTemplate"

isInterface :: TyClDecl GhcPs -> Maybe Typename
isInterface = hasGhcTypesConstraint "DamlInterface"

hasGhcTypesConstraint :: String -> TyClDecl GhcPs -> Maybe Typename
hasGhcTypesConstraint c decl
  | DataDecl {tcdLName, tcdDataDefn} <- decl
  , HsDataDefn {dd_ctxt} <- tcdDataDefn
  , L _ [L _ (HsTyVar _ _ (L _ (Qual mod cst)))] <- dd_ctxt
  , moduleNameString mod == "GHC.Types"
  , occNameString cst == c = Just $ Typename $ packRdrName $ unLoc tcdLName
  | otherwise = Nothing

-- | If the given instance declaration is declaring a template/interface choice instance,
--   return template/interface and choice name (IdP). Used to build the set of choices
--   per template/interface declared in a module.
isChoice :: ClsInstDecl GhcPs -> Maybe (Typename, Typename)
isChoice (XClsInstDecl _) = Nothing
isChoice ClsInstDecl{..}
  | L _ ty <- getLHsInstDeclHead cid_poly_ty
  = isChoiceTy ty

isChoiceTy :: HsType GhcPs -> Maybe (Typename, Typename)
isChoiceTy ty
  | HsAppTy _ (L _ cApp1) (L _ _cArgs) <- ty
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

-- | Get (normal) typeclass instances data.
getInstanceDocs :: DocCtx -> ClsInst -> InstanceDoc
getInstanceDocs ctx@DocCtx{dc_decls} ClsInst{..} =
    let ty = varType is_dfun
        srcSpan = getLoc $ idName is_dfun
        modname = Modulename $ T.pack $ moduleNameString $ moduleName $ nameModule is_cls_nm
        instDocMap = MS.fromList [(l, doc) | (DeclData (L l (InstD _x _i)) (Just doc)) <- dc_decls]
    in InstanceDoc
        { id_context = typeToContext ctx ty
        , id_module = modname
        , id_type = typeToType ctx ty
        , id_isOrphan = isOrphan is_orphan
        , id_descr = MS.lookup srcSpan instDocMap
        }

-- Utilities common to templates and interfaces
-----------------------------------------------

-- | Create an ADT from a Typename
asADT :: MS.Map Typename ADTDoc -> Typename -> ADTDoc
asADT typeMap n = fromMaybe dummyDT $ MS.lookup n typeMap
  where
    dummyDT =
      ADTDoc
        { ad_anchor = Nothing
        , ad_name = dummyName n
        , ad_descr = Nothing
        , ad_args = []
        , ad_constrs = []
        , ad_instances = Nothing
        }
    dummyName (Typename "Archive") = Typename "Archive"
    dummyName (Typename t) = Typename $ "External:" <> t

-- | Assuming one constructor (record or prefix), extract the fields, if any.  For choices without
-- arguments, GHC returns a prefix constructor, so we need to cater for this case specially.
getFields :: ADTDoc -> [FieldDoc]
getFields adt =
  case ad_constrs adt of
    [PrefixC {}] -> []
    [RecordC {ac_fields = fields}] -> fields
    [] -> [] -- catching the dummy case here, see above
    _other -> error "getFields: found multiple constructors"

mkChoiceDoc :: MS.Map Typename ADTDoc -> Typename -> ChoiceDoc
mkChoiceDoc typeMap name =
  ChoiceDoc
    { cd_name = ad_name choiceADT
    , cd_descr = ad_descr choiceADT
  -- assumes exactly one constructor (syntactic in the template syntax), or
  -- uses a dummy value otherwise.
    , cd_fields = getFields choiceADT
    }
  where
    choiceADT = asADT typeMap name
