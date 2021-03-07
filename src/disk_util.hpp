// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_CPP_DISK_UTIL_HPP_
#define SRC_CPP_DISK_UTIL_HPP_

#include <string>
#include <sstream>
#include <fstream>
#include <iostream>

#ifndef _WIN32
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include <stdlib.h>
#endif

#if !defined(_WIN32) && !defined(__APPLE__)
#include <sys/sysmacros.h>
#endif

#include "util.hpp"

namespace DiskUtil {

    static inline std::string DeviceNameOfDirectory(const std::string &dir)
    {
        struct stat s;
        memset(&s, 0, sizeof(s));

        if (0 != stat(dir.c_str(), &s)) {
            std::ostringstream err;
            err << "Unable to find device name for dir " << dir << ": "
                << strerror(errno);
            throw std::runtime_error(err.str());
        }

        std::ostringstream os;
        os << "/sys/dev/block/" << major(s.st_dev) << ":" << minor(s.st_dev);
        std::string block(os.str());

        char *resolved = realpath(block.c_str(), nullptr);
        if (resolved == nullptr) {
            std::ostringstream err;
            err << "Unable to find device name for " << block << ": "
                << strerror(errno);
            throw std::runtime_error(err.str());
        }
        std::string path(resolved);
        free(resolved);

        return path;
    }

    // For SSDs parallel writing is preferred.
    // For HDDs it's slow and causes fragmentation.
    static inline bool IsParallelWritingPreferredForDevice(
        const std::string &dev)
    {
        std::ostringstream os;
        os << "/sys/block/" << dev << "/queue/rotational";
        std::string filename = os.str();

        std::ifstream file;
        file.open(filename.c_str());

        if (file.fail()) {
            std::ostringstream err;
            err << "Unable to open " << filename << " for reading: "
                << strerror(errno);
            throw std::runtime_error(err.str());
        }

        std::string line;
        getline(file, line);

        file.close();

        int is_rotational = 1;
        if (!line.empty() && line.front() == '0') {
            is_rotational = 0;
        }

        return !is_rotational;
    }

    inline bool ShouldLock(const std::string &dir) {
#if defined(__APPLE__) || defined(_WIN32)
        return false;
#else
        std::string device_name = DiskUtil::DeviceNameOfDirectory(dir);
        bool is_parallel = DiskUtil::IsParallelWritingPreferredForDevice(
            device_name.c_str());

        return !is_parallel;
#endif
    }
}

class DirectoryLock
{
public:
    DirectoryLock(const std::string &dirname, bool lock = true)
    {
        dirname_ = dirname;
        if (lock) {
            Lock();
        }
    }

    virtual ~DirectoryLock()
    {
        Unlock();
    }

    bool Lock()
    {
        if (fd_ == -1) {
            fd_ = LockDirectory(dirname_);
        }
        return fd_ != -1;
    }
    
    bool Unlock()
    {
        if (fd_ == -1) {
            return false;
        }
        if (!UnlockDirectory(fd_, dirname_)) {
            return false;
        }
        fd_ = -1;
        return true;
    }

private:
    static int LockDirectory(
        std::string dirname)
    {
        int dir_fd = open(dirname.c_str(), O_RDONLY | O_NOCTTY);
        if (dir_fd == -1) {
            std::cerr << "Unable to open directory for locking: " << dirname
                << ". Error: " << strerror(errno) << std::endl;
            return -1;
        }
        while (0 != flock(dir_fd, LOCK_EX | LOCK_NB)) {
            if (EWOULDBLOCK == errno) {
                std::cout << "Directory locked, waiting (retrying in 1 minute): " << dirname << std::endl;
            } else {
                std::cerr << "Unable to lock directory (retrying in 1 minute): "
                    << ". Error: " << strerror(errno) << std::endl;
            }
            Util::sleep_seconds(1 * 60);
        }
        return dir_fd;
    }

    static bool UnlockDirectory(
        int dir_fd,
        std::string dirname)
    {
        if (-1 == flock(dir_fd, LOCK_UN)) {
            std::cerr << "Failed to unlock the directory: " << dirname
                << ". Error: " << strerror(errno) << std::endl;
            return false;
        }
        if (-1 == close(dir_fd)) {
            std::cerr << "Failed to close the directory during unlocking: " << dirname
                << ". Error: " << strerror(errno) << std::endl;
            return false;
        }
        return true;
	}

    int fd_ = -1;
    std::string dirname_;
};

#endif // SRC_CPP_DISK_UTIL_HPP_



