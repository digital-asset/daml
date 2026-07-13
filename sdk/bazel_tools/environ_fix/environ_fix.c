// The hermetic @llvm glibc split the environ/__environ weak alias into two
// distinct objects, so a GHC executable's copy-relocated `environ` stays NULL
// while libc keeps the real environment in `__environ`. That breaks
// getEnvironment AND env inheritance to spawned subprocesses (a child then
// sees an empty environment — e.g. no RUNFILES_DIR, so it can't locate its
// runfiles). Repoint `environ` at libc's real `__environ`, fetched via dlsym to
// bypass the copy-relocation. Runs before main (constructor); alwayslink keeps
// it from being GC'd since nothing references it. No-op on a correct glibc
// (same address) or where the symbol is absent (dlsym -> NULL), e.g. macOS.
#define _GNU_SOURCE  // for RTLD_DEFAULT
#include <dlfcn.h>

extern char **environ;

// Callable so a test that setenv's a NEW variable at runtime (which can realloc
// __environ, leaving the copy-relocated `environ` pointing at the old array) can
// re-sync before reading the environment back via getEnvironment.
void da_fix_environ(void) {
    char ***real = (char ***)dlsym(RTLD_DEFAULT, "__environ");
    if (real != (char ***)0) {
        environ = *real;
    }
}

__attribute__((constructor)) static void da_fix_environ_ctor(void) {
    da_fix_environ();
}
