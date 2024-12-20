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

#include "peer.hxx"

using namespace cornerstone;

void peer::send_req(ptr<req_msg>& req, rpc_handler& handler)
{
    // 感觉其实可以不用 async_result，因为没有wait吧
    ptr<rpc_result> pending = cs_new<rpc_result>(handler);
    // 这里又包裹了一层，先触发 h 再触发 handler
    rpc_handler h = [this, req, pending](ptr<resp_msg>& resp, const ptr<rpc_exception>& ex) mutable
    { this->handle_rpc_result(req, pending, resp, ex); };
    rpc_->send(req, h);
}

void peer::handle_rpc_result(
    ptr<req_msg>& req,
    ptr<rpc_result>& pending_result,
    ptr<resp_msg>& resp,
    const ptr<rpc_exception>& err)
{
    if (req->get_type() == msg_type::append_entries_request || req->get_type() == msg_type::install_snapshot_request)
    {
        set_free();
    }

    if (err == nilptr)
    {
        resume_hb_speed();
        pending_result->set_result(resp, ptr<rpc_exception>());
    }
    else
    {
        // 降低心跳检测频率
        slow_down_hb();
        pending_result->set_result(ptr<resp_msg>(), err);
    }
}