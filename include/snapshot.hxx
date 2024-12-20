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

#ifndef _SNAPSHOT_HXX_
#define _SNAPSHOT_HXX_

#include "buffer.hxx"
#include "cluster_config.hxx"

namespace cornerstone
{
class snapshot
{
public:
    /**
     * @param   last_log_idx        生成快照的最后日志idx
     * @param   last_log_term       生成快照的最后日志所属任期
     */
    snapshot(ulong last_log_idx, ulong last_log_term, const ptr<cluster_config>& last_config, ulong size = 0)
        : last_log_idx_(last_log_idx), last_log_term_(last_log_term), size_(size), last_config_(last_config)
    {
    }

    __nocopy__(snapshot);

public:
    ulong get_last_log_idx() const
    {
        return last_log_idx_;
    }

    ulong get_last_log_term() const
    {
        return last_log_term_;
    }

    ulong size() const
    {
        return size_;
    }

    const ptr<cluster_config>& get_last_config() const
    {
        return last_config_;
    }

    static ptr<snapshot> deserialize(buffer& buf);

    bufptr serialize();

private:
    ulong last_log_idx_;                // 快照最后一个log的idx
    ulong last_log_term_;               // 快照最后一个log的任期
    ulong size_;                        // 快照字节长度
    ptr<cluster_config> last_config_;   // 该快照最新集群配置
};
} // namespace cornerstone

#endif