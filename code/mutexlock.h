#ifndef TABLE_STORAGE_MUTEXLOCK_H
#define TABLE_STORAGE_MUTEXLOCK_H

#include <mutex>

class Mutex {
public:
    Mutex() = default;
    ~Mutex() = default;

    Mutex(const Mutex &) = delete;
    Mutex &operator=(const Mutex &) = delete;

    void Lock() { mu_.lock(); }
    void Unlock() { mu_.unlock(); }

private:
    std::mutex mu_;
};

class MutexLock {
public:
    MutexLock(Mutex *mu):mu_(mu) { mu_->Lock(); }
    ~MutexLock() { mu_->Unlock(); }
private:
    Mutex *mu_;
};

#endif /* TABLE_STORAGE_MUTEXLOCK_H */
