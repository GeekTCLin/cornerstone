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

#ifndef _REG_MSG_HXX_
#define _REG_MSG_HXX_

#include <vector>
#include "basic_types.hxx"
#include "log_entry.hxx"
#include "msg_base.hxx"
#include "pp_util.hxx"

namespace cornerstone
{
class req_msg : public msg_base
{
public:
    req_msg(ulong term, msg_type type, int32 src, int32 dst, ulong last_log_term, ulong last_log_idx, ulong commit_idx)
        : msg_base(term, type, src, dst),
          last_log_term_(last_log_term),
          last_log_idx_(last_log_idx),
          commit_idx_(commit_idx),
          log_entries_()
    {
    }

    virtual ~req_msg() __override__
    {
    }

    __nocopy__(req_msg);

public:
    ulong get_last_log_idx() const
    {
        return last_log_idx_;
    }

    ulong get_last_log_term() const
    {
        return last_log_term_;
    }

    ulong get_commit_idx() const
    {
        return commit_idx_;
    }

    std::vector<ptr<log_entry>>& log_entries()
    {
        return log_entries_;
    }

private:
    ulong last_log_term_;       // 最新日志的任期
    ulong last_log_idx_;        // 最新日志的下标
    ulong commit_idx_;          // 提交的log 下标
    std::vector<ptr<log_entry>> log_entries_;
};
} // namespace cornerstone

#endif //_REG_MSG_HXX_