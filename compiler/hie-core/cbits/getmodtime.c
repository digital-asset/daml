// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#include <sys/stat.h>
#include <time.h>
int getmodtime(const char* pathname, time_t* sec, long* nsec) {
    struct stat s;
    int r = stat(pathname, &s);
    if (r != 0) {
        return r;
    }
    *sec = s.st_mtim.tv_sec;
    *nsec = s.st_mtim.tv_nsec;
    return 0;
}

