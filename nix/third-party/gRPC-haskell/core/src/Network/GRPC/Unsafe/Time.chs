module Network.GRPC.Unsafe.Time where

import Control.Exception (bracket)
import Control.Monad
import Foreign.Storable
import System.Clock

#include <grpc/support/time.h>
#include <grpc_haskell.h>

{#context prefix = "grp" #}

newtype CTimeSpec = CTimeSpec { timeSpec :: TimeSpec }
  deriving (Eq, Show)

instance Storable CTimeSpec where
  sizeOf _ = {#sizeof gpr_timespec #}
  alignment _ = {#alignof gpr_timespec #}
  peek p = fmap CTimeSpec $ TimeSpec
    <$> liftM fromIntegral ({#get gpr_timespec->tv_sec #} p)
    <*> liftM fromIntegral ({#get gpr_timespec->tv_nsec #} p)
  poke p x = do
    {#set gpr_timespec.tv_sec #} p (fromIntegral $ sec $ timeSpec x)
    {#set gpr_timespec.tv_nsec #} p (fromIntegral $ nsec $ timeSpec x)

{#enum gpr_clock_type as ClockType {underscoreToCase} deriving (Eq) #}

-- | A pointer to a CTimeSpec. Must be destroyed manually with
-- 'timespecDestroy'.
{#pointer *gpr_timespec as CTimeSpecPtr -> CTimeSpec #}

{#fun unsafe timespec_destroy as ^ {`CTimeSpecPtr'} -> `()'#}

{#fun gpr_inf_future_ as ^ {`ClockType'} -> `CTimeSpecPtr'#}

-- | Get the current time for the given 'ClockType'. Warning: 'GprTimespan' will
-- cause a crash. Probably only need to use GprClockMonotonic, which returns 0.
{#fun gpr_now_ as ^ {`ClockType'} -> `CTimeSpecPtr'#}

{#fun gpr_time_to_millis_ as ^ {`CTimeSpecPtr'} -> `Int'#}

-- | Returns a GprClockMonotonic representing a deadline n seconds in the
-- future.
{#fun seconds_to_deadline as ^ {`Int'} -> `CTimeSpecPtr'#}

withDeadlineSeconds :: Int -> (CTimeSpecPtr -> IO a) -> IO a
withDeadlineSeconds i = bracket (secondsToDeadline i) timespecDestroy

-- | Returns a GprClockMonotonic representing a deadline n milliseconds
-- in the future.
{#fun millis_to_deadline as ^ {`Int'} -> `CTimeSpecPtr'#}

-- | Returns a GprClockMonotonic representing an infinitely distant deadline.
-- wraps gpr_inf_future in the gRPC library.
{#fun unsafe infinite_deadline as ^ {} -> `CTimeSpecPtr'#}

withInfiniteDeadline :: (CTimeSpecPtr -> IO a) -> IO a
withInfiniteDeadline = bracket infiniteDeadline timespecDestroy

{#fun convert_clock_type as ^ {`CTimeSpecPtr', `ClockType'} -> `CTimeSpecPtr'#}

withConvertedClockType :: CTimeSpecPtr -> ClockType
                          -> (CTimeSpecPtr -> IO a)
                          -> IO a
withConvertedClockType cptr ctype = bracket (convertClockType cptr ctype)
                                            timespecDestroy
