/*
 * Copyright (c) 2016 - present Alpha Infra Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _SNAPSHOT_SYNC_REQ_HXX_
#define _SNAPSHOT_SYNC_REQ_HXX_

#include "snapshot.hxx"

namespace cornerstone
{
class snapshot_sync_req
{
public:
    snapshot_sync_req(const ptr<snapshot>& s, ulong offset, bufptr&& buf, bool done)
        : snapshot_(s), offset_(offset), data_(std::move(buf)), done_(done)
    {
    }

    __nocopy__(snapshot_sync_req);

public:
    static ptr<snapshot_sync_req> deserialize(buffer& buf);

    snapshot& get_snapshot() const
    {
        return *snapshot_;
    }

    ulong get_offset() const
    {
        return offset_;
    }

    buffer& get_data() const
    {
        return *data_;
    }

    bool is_done() const
    {
        return done_;
    }

    bufptr serialize();

private:
    ptr<snapshot> snapshot_;
    ulong offset_;              // 字节偏移量
    bufptr data_;
    bool done_;                 // 是否结束，本次发送数据达到快照尾部
};
} // namespace cornerstone

#endif //_SNAPSHOT_SYNC_REQ_HXX_
