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

#ifndef _PEER_HXX_
#define _PEER_HXX_

#include "context.hxx"
#include "snapshot_sync_ctx.hxx"
#include "timer_task.hxx"

namespace cornerstone
{
class peer
{
public:
    peer(ptr<srv_config>& config, const context& ctx, timer_task<peer&>::executor& hb_exec)
        : config_(config),
          scheduler_(ctx.scheduler_),
          rpc_(ctx.rpc_cli_factory_->create_client(config->get_endpoint())),
          current_hb_interval_(ctx.params_->heart_beat_interval_),
          hb_interval_(ctx.params_->heart_beat_interval_),
          rpc_backoff_(ctx.params_->rpc_failure_backoff_),
          max_hb_interval_(ctx.params_->max_hb_interval()),
          next_log_idx_(0),
          matched_idx_(0),
          last_resp_(),
          busy_flag_(false),
          pending_commit_flag_(false),
          hb_enabled_(false),
          hb_task_(cs_new<timer_task<peer&>, timer_task<peer&>::executor&, peer&>(hb_exec, *this)),
          snp_sync_ctx_(),
          lock_()
    {
    }

    __nocopy__(peer);

public:
    int32 get_id() const
    {
        return config_->get_id();
    }

    const srv_config& get_config()
    {
        return *config_;
    }

    ptr<delayed_task>& get_hb_task()
    {
        return hb_task_;
    }

    std::mutex& get_lock()
    {
        return lock_;
    }

    int32 get_current_hb_interval() const
    {
        return current_hb_interval_;
    }

    bool make_busy()
    {
        bool f = false;
        return busy_flag_.compare_exchange_strong(f, true);
    }

    void set_free()
    {
        busy_flag_.store(false);
    }

    bool is_hb_enabled() const
    {
        return hb_enabled_;
    }

    void enable_hb(bool enable)
    {
        hb_enabled_ = enable;
        if (!enable)
        {
            scheduler_->cancel(hb_task_);
        }
    }

    ulong get_next_log_idx() const
    {
        return next_log_idx_;
    }

    ulong get_last_log_idx() const
    {
        return next_log_idx_ - 1;
    }

    void set_next_log_idx(ulong idx)
    {
        next_log_idx_ = idx;
    }

    ulong get_matched_idx() const
    {
        return matched_idx_;
    }

    void set_matched_idx(ulong idx)
    {
        matched_idx_ = idx;
    }

    const time_point& get_last_resp() const
    {
        return last_resp_;
    }

    template <typename T>
    void set_last_resp(T&& value)
    {
        last_resp_ = std::forward<T>(value);
    }

    void set_pending_commit()
    {
        pending_commit_flag_.store(true);
    }

    // 这个返回值用于判断是否需要同步 commit
    bool clear_pending_commit()
    {
        bool t = true;
        // 如果成功将 pending_commit_flag_ 从 true 改成 false，返回 true
        // 若 pending_commit_flag_ 本身为 false，则返回false
        return pending_commit_flag_.compare_exchange_strong(t, false);
    }

    void set_snapshot_in_sync(const ptr<snapshot>& s)
    {
        if (s == nilptr)
        {
            snp_sync_ctx_.reset();
        }
        else
        {
            snp_sync_ctx_ = cs_new<snapshot_sync_ctx>(s);
        }
    }

    ptr<snapshot_sync_ctx> get_snapshot_sync_ctx() const
    {
        return snp_sync_ctx_;
    }

    void slow_down_hb()
    {
        current_hb_interval_ = std::min(max_hb_interval_, current_hb_interval_ + rpc_backoff_);
    }

    // 恢复原始心跳间隔
    void resume_hb_speed()
    {
        current_hb_interval_ = hb_interval_;
    }

    
    /// @brief          发送RPC 请求
    /// @param req      请求消息
    /// @param handler  回复时触发回调
    void send_req(ptr<req_msg>& req, rpc_handler& handler);

private:
    // 回调函数，当RPC 请求发回reponse时触发
    void handle_rpc_result(
        ptr<req_msg>& req,
        ptr<rpc_result>& pending_result,
        ptr<resp_msg>& resp,
        const ptr<rpc_exception>& err);

private:
    ptr<srv_config> config_;                    // 节点配置
    ptr<delayed_task_scheduler> scheduler_;     // 时间事件管理类，（用于peer注册心跳超时事件）
    ptr<rpc_client> rpc_;                       // rpc 客户端，处理网络连接、数据收发
    int32 current_hb_interval_;                 // 当前心跳检测时间间隔
    int32 hb_interval_;                         // 心跳检测间隔
    int32 rpc_backoff_;                         // 心跳检测变化增量
    int32 max_hb_interval_;                     // 最大心跳检测间隔
    ulong next_log_idx_;                        // 期待接收的下一个log id
    ulong matched_idx_;                         // 匹配的id
    time_point last_resp_;                      // 上次peer节点回复时间
    std::atomic_bool busy_flag_;                // busy_flag_ 为true时，代表发送了 append_entries 或者          
                                                // install_snapshot_request 请求，需要进行等待？？？
                                                // 所以不能连续 append_entries，必须等待上次请求返回才能执行
    std::atomic_bool pending_commit_flag_;      // 是否需要同步 commit
    bool hb_enabled_;                           // 是否开启心跳检测
    ptr<delayed_task> hb_task_;                 // 心跳事件
    ptr<snapshot_sync_ctx> snp_sync_ctx_;       // 快照管理
    std::mutex lock_;
};
} // namespace cornerstone

#endif //_PEER_HXX_
