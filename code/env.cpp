#include "env.h"

#ifndef WIN32
#include "posix_env.h"
#else
#include "windows_env.h"
#endif // WIN32

Env* CreateEnv()
{
#ifndef WIN32
    return new PosixEnv();
#else
    return new WindowsEnv();
#endif
}