-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Extract.Templates
    ( getTemplateDocs
    , getTemplateData
    , getInstanceDocs
    , getInterfaceDocs
    , stripInstanceSuffix
    ) where

import DA.Daml.Doc.Types
import qualified DA.Daml.Doc.Types as DDoc
import DA.Daml.Doc.Extract.Types
import DA.Daml.Doc.Extract.Util
import DA.Daml.Doc.Extract.TypeExpr

import qualified Data.Map.Strict as MS
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Tuple.Extra (second)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Monoid (First (..))

import "ghc-lib" GHC
import "ghc-lib-parser" Var (varType)
import "ghc-lib-parser" CoreSyn (isOrphan)
import "ghc-lib-parser" InstEnv
import "ghc-lib-parser" OccName
import "ghc-lib-parser" Id

-- | Build template docs up from ADT and class docs.
getTemplateDocs ::
    DocCtx
    -> MS.Map Typename ADTDoc -- ^ maps template names to their ADT docs
    -> MS.Map Typename (Set.Set DDoc.Type)-- ^ maps template names to their implemented interfaces' types
    -> [TemplateDoc]
getTemplateDocs DocCtx{..} typeMap templateImplementsMap =
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
      , td_impls =
          ImplDoc <$>
            Set.toList (MS.findWithDefault mempty name templateImplementsMap)
     }
      where
        tmplADT = asADT typeMap name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices


-- | Build interface docs up from class docs.
getInterfaceDocs :: DocCtx
    -> MS.Map Typename ADTDoc -- ^ maps template names to their ADT docs
    -> [InterfaceDoc]
getInterfaceDocs DocCtx{..} typeMap =
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
      }
      where
        ifADT = asADT typeMap name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices

-- | Extracts all names of templates defined in a module,
-- and a map of template names to its set of choices
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
                mapMaybe isChoice instDs ++
                mapMaybe isIfaceChoice instDs
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

-- | If the given instance declaration is declaring a template choice instance,
--   return template and choice name (IdP). Used to build the set of choices
--   per template declared in a module.
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

-- | If the given instance declaration is declaring an interface choice instance, return interface
-- name and choice name.
isIfaceChoice :: ClsInstDecl GhcPs -> Maybe (Typename, Typename)
isIfaceChoice (XClsInstDecl _) = Nothing
isIfaceChoice decl@ClsInstDecl{}
  | Just (ifaceName, ty) <- hasImplementsConstraint decl
  , Just (_templ, choiceName) <- isChoiceTy ty
  = Just (Typename . packRdrName $ ifaceName, choiceName)

  | otherwise = Nothing

-- | Matches on a DA.Internal.Desugar.Implements interface constraint in the context of the instance
-- declaration. Returns the interface name and the body of the instance in case the constraint is
-- present, else nothing.
hasImplementsConstraint :: ClsInstDecl GhcPs -> Maybe (RdrName, HsType GhcPs)
hasImplementsConstraint (XClsInstDecl _) = Nothing
hasImplementsConstraint ClsInstDecl{..} =
  let (L _ ctxs, L _ ty) = splitLHsQualTy $ hsSigType cid_poly_ty
  in (,ty) <$> getFirst (foldMap (First . getImplementsConstraint) ctxs)

-- | If the given type is a (DA.Internal.Desugar.Implements t I) constraint,
-- returns the name of I.
getImplementsConstraint :: LHsType GhcPs -> Maybe RdrName
getImplementsConstraint lctx
  | L _ ctx <- dropParTy lctx
  , HsAppTy _ (L _ app1) (L _ iface) <- ctx
  , HsTyVar _ _ (L _ ifaceName) <- iface
  , HsAppTy _ (L _ impl) (L _ _t) <- app1
  , HsTyVar _ _ (L _ implCls) <- impl
  , Qual implClsModule implClassOcc <- implCls
  , moduleNameString implClsModule == "DA.Internal.Desugar"
  , occNameString implClassOcc == "Implements"
  = Just ifaceName
  | otherwise = Nothing

-- | Removes any `HsParTy` constructors from an `LHsType a`.
dropParTy :: LHsType a -> LHsType a
dropParTy (L _ (HsParTy _ ty)) = dropParTy ty
dropParTy ty = ty

-- | Strip the @Instance@ suffix off of a typename, if it's there.
-- Otherwise returns 'Nothing'.
stripInstanceSuffix :: Typename -> Maybe Typename
stripInstanceSuffix (Typename t) = Typename <$> T.stripSuffix "Instance" t

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
