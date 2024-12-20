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

#ifndef _RAFT_SERVER_HXX_
#define _RAFT_SERVER_HXX_

#include <shared_mutex>
#include <unordered_map>
#include "context.hxx"
#include "peer.hxx"
#include "snapshot_sync_ctx.hxx"
#include "snapshot_sync_req.hxx"
#include "srv_role.hxx"

namespace cornerstone
{
class raft_server
{
public:
    raft_server(context* ctx);
    virtual ~raft_server();

    __nocopy__(raft_server);

public:
    ptr<resp_msg> process_req(req_msg& req);

    ptr<async_result<bool>> add_srv(const srv_config& srv);

    ptr<async_result<bool>> remove_srv(const int srv_id);

    /**
     * appends log entries to cluster.
     * all log buffer pointers will be moved and become invalid after this returns.
     * this will automatically forward the request to leader if current node is not
     * the leader, so that no log entry cookie is allowed since the log entries may
     * be serialized and transfer to current leader.
     * @return async result that indicates the log entries have been accepted or not
     */
    ptr<async_result<bool>> append_entries(std::vector<bufptr>& logs);

    /**
     * replicates a log entry to cluster.
     * log buffer pointer will be moved and become invalid after this returns.
     * @return true if current node is the leader and log entry is accepted.
     */
    bool replicate_log(bufptr& log, const ptr<void>& cookie, uint cookie_tag);

    bool is_leader() const;

private:
    typedef std::unordered_map<int32, ptr<peer>>::const_iterator peer_itor;

private:
    ptr<resp_msg> handle_append_entries(req_msg& req);
    ptr<resp_msg> handle_vote_req(req_msg& req);
    ptr<resp_msg> handle_cli_req(req_msg& req);
    ptr<resp_msg> handle_extended_msg(req_msg& req);
    ptr<resp_msg> handle_install_snapshot_req(req_msg& req);
    ptr<resp_msg> handle_rm_srv_req(req_msg& req);
    ptr<resp_msg> handle_add_srv_req(req_msg& req);
    ptr<resp_msg> handle_log_sync_req(req_msg& req);
    ptr<resp_msg> handle_join_cluster_req(req_msg& req);
    ptr<resp_msg> handle_leave_cluster_req(req_msg& req);
    ptr<resp_msg> handle_prevote_req(req_msg& req);
    bool handle_snapshot_sync_req(snapshot_sync_req& req);
    void request_vote();
    void request_prevote();
    void request_append_entries();
    bool request_append_entries(peer& p);
    void handle_peer_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err);
    void handle_append_entries_resp(resp_msg& resp);
    void handle_install_snapshot_resp(resp_msg& resp);
    void handle_voting_resp(resp_msg& resp);
    void handle_ext_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err);
    void handle_ext_resp_err(rpc_exception& err);
    void handle_prevote_resp(resp_msg& resp);
    ptr<req_msg> create_append_entries_req(peer& p);
    ptr<req_msg> create_sync_snapshot_req(peer& p, ulong last_log_idx, ulong term, ulong commit_idx);
    void commit(ulong target_idx);
    void snapshot_and_compact(ulong committed_idx);
    bool update_term(ulong term);
    void reconfigure(const ptr<cluster_config>& new_config);
    void become_candidate();
    void become_leader();
    void become_follower();
    void enable_hb_for_peer(peer& p);
    void restart_election_timer();
    void stop_election_timer();
    void handle_hb_timeout(peer& peer);
    void handle_election_timeout();
    void sync_log_to_new_srv(ulong start_idx);
    void invite_srv_to_join_cluster();
    void rm_srv_from_cluster(int32 srv_id);
    int get_snapshot_sync_block_size() const;
    void on_snapshot_completed(ptr<snapshot>& s, bool result, const ptr<std::exception>& err);
    void on_retryable_req_err(ptr<peer>& p, ptr<req_msg>& req);
    ulong term_for_log(ulong log_idx);
    void commit_in_bg();
    ptr<async_result<bool>> send_msg_to_leader(ptr<req_msg>& req);

private:
    static const int default_snapshot_sync_block_size;
    
    int32 leader_;                                  // leaderId
    int32 id_;                                      // 本服务器id

    int32 votes_granted_;                           // 投票数
    ulong quick_commit_idx_;                        // 可提交的idx
    ulong sm_commit_index_;                         // 已提交的idx
    bool election_completed_;                       // 选举是否结束
    bool config_changing_;
    bool catching_up_;                              // 是否正在追赶日志 新加入的服务器
    bool stopping_;
    int32 steps_to_down_;                           // 只有handle_leave_cluster_req 才会更改这个值

    std::atomic_bool snp_in_progress_;              // 是否正在执行快照生成，同一时刻只能存在一个快照的创建
    std::unique_ptr<context> ctx_;
    ptr<delayed_task_scheduler> scheduler_;         // 时间事件管理器
   
    timer_task<void>::executor election_exec_;      // 超时回调
    ptr<delayed_task> election_task_;               // 超时任务

    std::unordered_map<int32, ptr<peer>> peers_;                // leader 节点请求 peer节点使用
    mutable std::shared_timed_mutex peers_lock_;                // c++14 17 支持
    std::unordered_map<int32, ptr<rpc_client>> rpc_clients_;    // 客户端连接，主要为follower 节点连接leader时会创建
    srv_role role_;                                 // 本服务器角色枚举
    
    ptr<srv_state> state_;                          // 服务器状态（含任期、投票目标）
    ptr<log_store> log_store_;                      // 日志存储
    ptr<state_machine> state_machine_;              // 状态机（存储引擎）
    ptr<logger> l_;                                 // 日志
    std::function<int32()> rand_timeout_;           // 随机超时时间计算函数
    ptr<cluster_config> config_;                    // 集群配置
    ptr<peer> srv_to_join_; 
    ptr<srv_config> conf_to_add_;
    std::recursive_mutex lock_;
    std::mutex commit_lock_;                        // commit 锁，配合 commit_cv_ 等待线程唤醒
    std::mutex rpc_clients_lock_;
    std::condition_variable commit_cv_;             // 检测提交条件变量，当quick_commit_idx_ 大于 sm_commit_index_ 时唤醒bg线程
    std::mutex stopping_lock_;                      // 用于等待 commit 线程，配合ready_to_stop_cv_
    std::condition_variable ready_to_stop_cv_;
    rpc_handler resp_handler_;                      // resp 回调
    rpc_handler ex_resp_handler_;                   // 扩展 resp 回调
    ptr<snapshot> last_snapshot_;                   // 最新快照
    std::unordered_set<int32> voted_servers_;       // 投票的服务器id
    uptr<prevote_state> prevote_state_;

};
} // namespace cornerstone
#endif //_RAFT_SERVER_HXX_
