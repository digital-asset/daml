{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections      #-}

module Network.GRPC.Unsafe.Security where

import Control.Exception (bracket)
import Data.ByteString (ByteString, useAsCString, packCString, packCStringLen)
import Data.Coerce (coerce)
import Foreign.C.String (CString)
import Foreign.C.Types
import Foreign.Storable
import Foreign.Marshal.Alloc (free)
import Foreign.Ptr (nullPtr, FunPtr, Ptr, castPtr)

#include <grpc/grpc.h>
#include <grpc/grpc_security.h>
#include <grpc_haskell.h>

{#import Network.GRPC.Unsafe#}
{#import Network.GRPC.Unsafe.ChannelArgs#}
{#import Network.GRPC.Unsafe.Metadata#}
{#import Network.GRPC.Unsafe.Op#}

{#context prefix = "grpc_"#}

-- * Types

-- | Context for auth. This is essentially just a set of key-value pairs that
-- can be mutated.
-- Note: it appears that any keys set or modified on this object do not
-- appear in the AuthContext of the peer, so you must send along auth info
-- in the metadata. It's currently unclear to us what
-- the purpose of modifying this is, but we offer the ability for the sake of
-- completeness.
{#pointer *auth_context as ^ newtype#}

deriving instance Show AuthContext

instance Storable AuthContext where
  sizeOf (AuthContext p) = sizeOf p
  alignment (AuthContext p) = alignment p
  peek p = AuthContext <$> peek (castPtr p)
  poke p (AuthContext r) = poke (castPtr p) r

{#pointer *auth_property_iterator as ^ newtype#}

{#pointer *call_credentials as ^ newtype#}

{#pointer *channel_credentials as ^ newtype#}

{#pointer *server_credentials as ^ newtype#}

withAuthPropertyIterator :: AuthContext
                            -> (AuthPropertyIterator -> IO a)
                            -> IO a
withAuthPropertyIterator ctx = bracket (authContextPropertyIterator ctx)
                                       (free . coerce)

-- | Represents one key/value pair in an 'AuthContext'.
data AuthProperty = AuthProperty
  {authPropName :: ByteString,
   authPropValue :: ByteString}
  deriving (Show, Eq)

marshalAuthProperty :: Ptr AuthProperty -> IO AuthProperty
marshalAuthProperty p = do
  n <- packCString =<< ({# get auth_property->name #} p)
  vl <- fromIntegral <$> {# get auth_property->value_length #} p
  v <- packCStringLen . (,vl) =<< {#get auth_property->value #} p
  return $ AuthProperty n v

-- | The context which a client-side auth metadata plugin sees when it runs.
data AuthMetadataContext = AuthMetadataContext
  {serviceURL :: ByteString,
   -- ^ The URL of the service the current call is going to.
   methodName :: ByteString,
   -- ^ The method that is being invoked with the current call. It appears that
   -- the gRPC 0.15 core is not populating this correctly, because it's an empty
   -- string in my tests so far.
   channelAuthContext :: AuthContext
 }
  deriving Show

authMetadataContextMarshal :: Ptr AuthMetadataContext -> IO AuthMetadataContext
authMetadataContextMarshal p =
  AuthMetadataContext
   <$> (({#get auth_metadata_context->service_url #} p) >>= packCString)
   <*> (({#get auth_metadata_context->method_name #} p) >>= packCString)
   <*> ({#get auth_metadata_context->channel_auth_context#} p)

{#pointer *metadata_credentials_plugin as ^ newtype#}

{#pointer *auth_metadata_processor as ^ newtype#}

{#enum ssl_client_certificate_request_type as ^ {underscoreToCase}
  deriving (Eq, Ord, Bounded, Show)#}

-- * Auth Contexts

-- | If used, the 'AuthContext' must be released with 'AuthContextRelease'.
{#fun unsafe call_auth_context as ^
  {`Call'} -> `AuthContext'#}

{#fun unsafe auth_context_release as ^
  {`AuthContext'} -> `()'#}

{#fun unsafe auth_context_add_cstring_property as addAuthProperty'
  {`AuthContext',
   useAsCString* `ByteString',
   useAsCString* `ByteString'}
  -> `()'#}

-- | Adds a new property to the given 'AuthContext'.
addAuthProperty :: AuthContext -> AuthProperty -> IO ()
addAuthProperty ctx prop =
  addAuthProperty' ctx (authPropName prop) (authPropValue prop)

{-
TODO: The functions for getting and setting peer identities cause
unpredictable crashes when used in conjunction with other, more general
auth property getter/setter functions. If we end needing these, we should
investigate further.

coercePack :: Ptr a -> IO ByteString
coercePack = packCString . coerce

{#fun unsafe grpc_auth_context_peer_identity_property_name
  as getPeerIdentityPropertyName
  {`AuthContext'} -> `ByteString' coercePack* #}

{#fun unsafe auth_context_set_peer_identity_property_name
  as setPeerIdentity
  {`AuthContext', useAsCString* `ByteString'} -> `()'#}

{#fun unsafe auth_context_peer_identity as getPeerIdentity
  {`AuthContext'} -> `ByteString' coercePack* #}
-}
-- * Property Iteration

{#fun unsafe auth_context_property_iterator_ as ^
  {`AuthContext'} -> `AuthPropertyIterator'#}

{#fun unsafe auth_property_iterator_next as ^
  {`AuthPropertyIterator'} -> `Ptr AuthProperty' coerce#}

getAuthProperties :: AuthContext -> IO [AuthProperty]
getAuthProperties ctx = withAuthPropertyIterator ctx $ \i -> do
  go i
  where go :: AuthPropertyIterator -> IO [AuthProperty]
        go i = do p <- authPropertyIteratorNext i
                  if p == nullPtr
                     then return []
                     else do props <- go i
                             prop <- marshalAuthProperty p
                             return (prop:props)

-- * Channel Credentials

{#fun unsafe channel_credentials_release as ^
  {`ChannelCredentials'} -> `()'#}

{#fun unsafe composite_channel_credentials_create as ^
  {`ChannelCredentials', `CallCredentials',unReserved `Reserved'}
  -> `ChannelCredentials'#}

{#fun unsafe ssl_credentials_create_internal as ^
  {`CString', `CString', `CString'} -> `ChannelCredentials'#}

sslChannelCredentialsCreate :: Maybe ByteString
                               -> Maybe ByteString
                               -> Maybe ByteString
                               -> IO ChannelCredentials
sslChannelCredentialsCreate (Just s) Nothing Nothing =
  useAsCString s $ \s' -> sslCredentialsCreateInternal s' nullPtr nullPtr
sslChannelCredentialsCreate Nothing (Just s1) (Just s2) =
  useAsCString s1 $ \s1' -> useAsCString s2 $ \s2' ->
  sslCredentialsCreateInternal nullPtr s1' s2'
sslChannelCredentialsCreate (Just s1) (Just s2) (Just s3) =
  useAsCString s1 $ \s1' -> useAsCString s2 $ \s2' -> useAsCString s3 $ \s3' ->
  sslCredentialsCreateInternal s1' s2' s3'
sslChannelCredentialsCreate (Just s1) _ _ =
  useAsCString s1 $ \s1' ->
  sslCredentialsCreateInternal s1' nullPtr nullPtr
sslChannelCredentialsCreate _ _ _ =
  sslCredentialsCreateInternal nullPtr nullPtr nullPtr

withChannelCredentials :: Maybe ByteString
                          -> Maybe ByteString
                          -> Maybe ByteString
                          -> (ChannelCredentials -> IO a)
                          -> IO a
withChannelCredentials x y z = bracket (sslChannelCredentialsCreate x y z)
                                       channelCredentialsRelease

-- * Call Credentials

{#fun call_set_credentials as ^
  {`Call', `CallCredentials'} -> `CallError'#}

{#fun unsafe call_credentials_release as ^
  {`CallCredentials'} -> `()'#}

{#fun unsafe composite_call_credentials_create as ^
  {`CallCredentials', `CallCredentials', unReserved `Reserved'}
  -> `CallCredentials'#}

-- * Server Credentials

{#fun unsafe server_credentials_release as ^
  {`ServerCredentials'} -> `()'#}

{#fun ssl_server_credentials_create_internal as ^
  {`CString',
   useAsCString*  `ByteString',
   useAsCString* `ByteString',
   `SslClientCertificateRequestType'}
  -> `ServerCredentials'#}

sslServerCredentialsCreate :: Maybe ByteString
                              -- ^ PEM encoding of the client root certificates.
                              -- Can be 'Nothing' if SSL authentication of
                              -- clients is not desired.
                              -> ByteString
                              -- ^ Server private key.
                              -> ByteString
                              -- ^ Server certificate.
                              -> SslClientCertificateRequestType
                              -- ^ How to handle client certificates.
                              -> IO ServerCredentials
sslServerCredentialsCreate Nothing k c t =
  sslServerCredentialsCreateInternal nullPtr k c t
sslServerCredentialsCreate (Just cc) k c t =
  useAsCString cc $ \cc' -> sslServerCredentialsCreateInternal cc' k c t

withServerCredentials :: Maybe ByteString
                         -- ^ PEM encoding of the client root certificates.
                         -- Can be 'Nothing' if SSL authentication of
                         -- clients is not desired.
                         -> ByteString
                         -- ^ Server private key.
                         -> ByteString
                         -- ^ Server certificate.
                         -> SslClientCertificateRequestType
                         -- ^ How to handle client certificates.
                         -> (ServerCredentials -> IO a)
                         -> IO a
withServerCredentials a b c d = bracket (sslServerCredentialsCreate a b c d)
                                        serverCredentialsRelease

-- * Creating Secure Clients/Servers

{#fun server_add_secure_http2_port as ^
  {`Server',useAsCString*  `ByteString', `ServerCredentials'} -> `Int'#}

{#fun secure_channel_create as ^
  {`ChannelCredentials',useAsCString* `ByteString', `GrpcChannelArgs', unReserved `Reserved'}
  -> `Channel'#}

-- * Custom metadata processing -- server side

-- | Type synonym for the raw function pointer we pass to C to handle custom
-- server-side metadata auth processing.
type CAuthProcess = Ptr ()
                 -> AuthContext
                 -> MetadataKeyValPtr
                 -> CSize
                 -> FunPtr CDoneCallback
                 -> Ptr ()
                 -> IO ()

foreign import ccall "wrapper"
  mkAuthProcess :: CAuthProcess -> IO (FunPtr CAuthProcess)

type CDoneCallback = Ptr ()
                   -> MetadataKeyValPtr
                   -> CSize
                   -> MetadataKeyValPtr
                   -> CSize
                   -> CInt -- ^ statuscode
                   -> CString -- ^ status details
                   -> IO ()

foreign import ccall "dynamic"
  unwrapDoneCallback :: FunPtr CDoneCallback -> CDoneCallback

{#fun server_credentials_set_auth_metadata_processor_ as ^
  {`ServerCredentials', `AuthMetadataProcessor'} -> `()'#}

foreign import ccall "grpc_haskell.h mk_auth_metadata_processor"
  mkAuthMetadataProcessor :: FunPtr CAuthProcess -> IO AuthMetadataProcessor

data AuthProcessorResult = AuthProcessorResult
  { resultConsumedMetadata :: MetadataMap
  -- ^ Metadata to remove from the request before passing to the handler.
  , resultResponseMetadata :: MetadataMap
  -- ^ Metadata to add to the response.
  , resultStatus :: StatusCode
  -- ^ StatusOk if auth was successful. Using any other status code here will
  -- cause the request to be rejected without reaching a handler.
  -- For rejected requests, it's suggested that this
  -- be StatusUnauthenticated or StatusPermissionDenied.
  -- NOTE: if you are using the low-level interface and the request is rejected,
  -- then handling functions in the low-level
  -- interface such as 'serverHandleNormalCall' will not unblock until they
  -- receive another request that is not rejected. So, if you write a buggy
  -- auth plugin that rejects all requests, your server could hang.
  , resultStatusDetails :: StatusDetails}

-- | A custom auth metadata processor. This can be used to implement customized
-- auth schemes based on the metadata in the request.
type ProcessMeta = AuthContext
                   -> MetadataMap
                   -> IO AuthProcessorResult

convertProcessor :: ProcessMeta -> CAuthProcess
convertProcessor f = \_state authCtx inMeta numMeta callBack userDataPtr -> do
  meta <- getAllMetadata inMeta (fromIntegral numMeta)
  AuthProcessorResult{..} <- f authCtx meta
  let cb = unwrapDoneCallback callBack
  let status = (fromEnum resultStatus)
  withPopulatedMetadataKeyValPtr resultConsumedMetadata $ \(conMeta, conLen) ->
    withPopulatedMetadataKeyValPtr resultResponseMetadata $ \(resMeta, resLen) ->
    useAsCString (unStatusDetails resultStatusDetails) $ \dtls -> do
      cb userDataPtr
         conMeta
         (fromIntegral conLen)
         resMeta
         (fromIntegral resLen)
         (fromIntegral status)
         dtls

-- | Sets the custom metadata processor for the given server credentials.
setMetadataProcessor :: ServerCredentials -> ProcessMeta -> IO ()
setMetadataProcessor creds processor = do
  let rawProcessor = convertProcessor processor
  rawProcessorPtr <- mkAuthProcess rawProcessor
  metaProcessor <- mkAuthMetadataProcessor rawProcessorPtr
  serverCredentialsSetAuthMetadataProcessor creds metaProcessor

-- * Client-side metadata plugins

type CGetMetadata = Ptr AuthMetadataContext
                 -> FunPtr CGetMetadataCallBack
                 -> Ptr ()
                -- ^ user data ptr (opaque, but must be passed on)
                 -> IO ()

foreign import ccall "wrapper"
  mkCGetMetadata :: CGetMetadata -> IO (FunPtr CGetMetadata)

type CGetMetadataCallBack = Ptr ()
                         -> MetadataKeyValPtr
                         -> CSize
                         -> CInt
                         -> CString
                         -> IO ()

foreign import ccall "dynamic"
  unwrapGetMetadataCallback :: FunPtr CGetMetadataCallBack
                            -> CGetMetadataCallBack

data ClientMetadataCreateResult = ClientMetadataCreateResult
  { clientResultMetadata :: MetadataMap
    -- ^ Additional metadata to add to the call.
  , clientResultStatus   :: StatusCode
   -- ^ if not 'StatusOk', causes the call to fail with the given status code.
   -- NOTE: if the auth fails, the call will not get sent to the server. So, if
   -- you're writing a test, your server might wait for a request forever.
  , clientResultDetails  :: StatusDetails }

-- | Optional plugin for attaching custom auth metadata to each call.
type ClientMetadataCreate = AuthMetadataContext
                            -> IO ClientMetadataCreateResult

convertMetadataCreate :: ClientMetadataCreate -> CGetMetadata
convertMetadataCreate f = \authCtxPtr doneCallback userDataPtr -> do
  authCtx <- authMetadataContextMarshal authCtxPtr
  ClientMetadataCreateResult{..} <- f authCtx
  let cb = unwrapGetMetadataCallback doneCallback
  withPopulatedMetadataKeyValPtr clientResultMetadata $ \(meta,metaLen) ->
    useAsCString (unStatusDetails clientResultDetails) $ \details -> do
      let status = fromIntegral $ fromEnum clientResultStatus
      cb userDataPtr meta (fromIntegral metaLen) status details

foreign import ccall "grpc_haskell.h mk_metadata_client_plugin"
  mkMetadataClientPlugin :: FunPtr CGetMetadata -> IO MetadataCredentialsPlugin

{#fun metadata_credentials_create_from_plugin_ as ^
  {`MetadataCredentialsPlugin'} -> `CallCredentials' #}

createCustomCallCredentials :: ClientMetadataCreate -> IO CallCredentials
createCustomCallCredentials create = do
  let rawCreate = convertMetadataCreate create
  rawCreatePtr <- mkCGetMetadata rawCreate
  plugin <- mkMetadataClientPlugin rawCreatePtr
  metadataCredentialsCreateFromPlugin plugin
