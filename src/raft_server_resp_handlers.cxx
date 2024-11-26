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

#include "raft_server.hxx"
#include "strfmt.hxx"

using namespace cornerstone;

extern const char* __msg_type_str[];

void raft_server::handle_peer_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err)
{
    if (err)
    {
        l_->info(sstrfmt("peer response error: %s").fmt(err->what()));
        return;
    }

    // update peer last response time
    {
        read_lock(peers_lock_);
        auto peer = peers_.find(resp->get_src());
        if (peer != peers_.end())
        {
            peer->second->set_last_resp(system_clock::now());
        }
        else
        {
            l_->info(sstrfmt("Peer %d not found, ignore the message").fmt(resp->get_src()));
            return;
        }
    }

    l_->debug(lstrfmt("Receive a %s message from peer %d with Result=%d, Term=%llu, NextIndex=%llu")
                  .fmt(
                      __msg_type_str[resp->get_type()],
                      resp->get_src(),
                      resp->get_accepted() ? 1 : 0,
                      resp->get_term(),
                      resp->get_next_idx()));

    {
        recur_lock(lock_);
        // if term is updated, no more action is required
        if (update_term(resp->get_term()))
        {
            return;
        }

        // ignore the response that with lower term for safety
        switch (resp->get_type())
        {
            case msg_type::vote_response:
                handle_voting_resp(*resp);
                break;
            case msg_type::append_entries_response:
                handle_append_entries_resp(*resp);
                break;
            case msg_type::install_snapshot_response:
                handle_install_snapshot_resp(*resp);
                break;
            default:
                l_->err(sstrfmt("Received an unexpected message %s for response, system exits.")
                            .fmt(__msg_type_str[resp->get_type()]));
                ctx_->state_mgr_->system_exit(-1);
                ::exit(-1);
                break;
        }
    }
}

/**
 * 1.   判断peer 是否存在
 * 2.   请求 同意，peer 节点接收了日志
 *      更新peer next_log_idx 以及 matched_idx
 *      matched_idx 更新 判断 是否进行commit提交
 * 3.   请求 失败，peer 拒绝了日志
 *      更新peer next_log_idx 若resp 有设定则更新，反之从当前记录的 next_log_idx 向前-1 逆推日志进行试探
 * 4.   若peer 节点日志落后于当前节点log_store，或者需要同步peer commit，进行追加不等待 心跳或者cli新请求
 */
void raft_server::handle_append_entries_resp(resp_msg& resp)
{
    read_lock(peers_lock_);
    peer_itor it = peers_.find(resp.get_src());
    /** 
    * handle_peer_resp 有检测，理论可以不检测了
    if (it == peers_.end())
    {
        l_->info(sstrfmt("the response is from an unkonw peer %d").fmt(resp.get_src()));
        return;
    }
    */

    // if there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
    bool need_to_catchup = true;
    ptr<peer> p = it->second;
    if (resp.get_accepted())
    {
        {
            auto_lock(p->get_lock());
            p->set_next_log_idx(resp.get_next_idx());
            p->set_matched_idx(resp.get_next_idx() - 1);
        }

        // try to commit with this response
        std::vector<ulong> matched_indexes(peers_.size() + 1);
        matched_indexes[0] = log_store_->next_slot() - 1;
        int i = 1;
        for (it = peers_.begin(); it != peers_.end(); ++it, i++)
        {
            matched_indexes[i] = it->second->get_matched_idx();
        }
        std::sort(matched_indexes.begin(), matched_indexes.end(), std::greater<ulong>());
        commit(matched_indexes[(peers_.size() + 1) / 2]);

        // 如果 需要同步commit 或者 peer 节点的期待日志 小于 当前存储的 日志，进行追加发送
        need_to_catchup = p->clear_pending_commit() || resp.get_next_idx() < log_store_->next_slot();
    }
    else
    {
        // 修正 peer next_idx
        // 如果resp 有发回接收的next_idx 则直接修改，反之 --1 持续试探
        std::lock_guard<std::mutex> guard(p->get_lock());
        if (resp.get_next_idx() > 0 && p->get_next_log_idx() > resp.get_next_idx())
        {
            // fast move for the peer to catch up
            p->set_next_log_idx(resp.get_next_idx());
        }
        else if (p->get_next_log_idx() > 0)
        {
            p->set_next_log_idx(p->get_next_log_idx() - 1);
        }
    }

    // This may not be a leader anymore, such as the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == srv_role::leader && need_to_catchup)
    {
        //Maybe Error 这里如果make_busy 失败，是否会导致 clear_pending_commit 错误消除了 pending_commit_flag_ ????
        request_append_entries(*p);
    }
}

void raft_server::handle_install_snapshot_resp(resp_msg& resp)
{
    read_lock(peers_lock_);
    peer_itor it = peers_.find(resp.get_src());
    /**
    * handle_peer_resp 有检测，理论可以不检测了
    if (it == peers_.end())
    {
        l_->info(sstrfmt("the response is from an unkonw peer %d").fmt(resp.get_src()));
        return;
    }
    */

    // if there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
    bool need_to_catchup = true;
    ptr<peer> p = it->second;
    if (resp.get_accepted())
    {
        std::lock_guard<std::mutex> guard(p->get_lock());
        ptr<snapshot_sync_ctx> sync_ctx = p->get_snapshot_sync_ctx();
        if (sync_ctx == nilptr)
        {
            l_->info("no snapshot sync context for this peer, drop the response");
            need_to_catchup = false;
        }
        else
        {
            if (resp.get_next_idx() >= sync_ctx->get_snapshot()->size())
            {
                // 理论上 resp.get_next_idx() 应该是 == sync_ctx->get_snapshot()->size()
                l_->debug("snapshot sync is done");
                // 更新 peer 节点 next_log_idx 下一个期待id 和 提交id
                p->set_next_log_idx(sync_ctx->get_snapshot()->get_last_log_idx() + 1);
                p->set_matched_idx(sync_ctx->get_snapshot()->get_last_log_idx());

                ptr<snapshot> nil_snp;
                p->set_snapshot_in_sync(nil_snp);
                // 如果有 commit 需要同步 或者 id 落后于 log_store_
                need_to_catchup = p->clear_pending_commit() || resp.get_next_idx() < log_store_->next_slot();
            }
            else
            {
                // 需要继续同步快照
                l_->debug(sstrfmt("continue to sync snapshot at offset %llu").fmt(resp.get_next_idx()));
                // 更新偏移量
                sync_ctx->set_offset(resp.get_next_idx());
            }
        }
    }
    else
    {
        l_->info("peer declines to install the snapshot, will retry");
    }

    // This may not be a leader anymore, such as the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == srv_role::leader && need_to_catchup)
    {
        request_append_entries(*p);
    }
}

// 处理投票响应
void raft_server::handle_voting_resp(resp_msg& resp)
{
    // 任期需要比较，不同任期的投票不能接收
    if (resp.get_term() != state_->get_term())
    {
        l_->info(sstrfmt("Received an outdated vote response at term %llu v.s. current term %llu")
                     .fmt(resp.get_term(), state_->get_term()));
        return;
    }

    // 选举已经结束了
    if (election_completed_)
    {
        l_->info("Election completed, will ignore the voting result from this server");
        return;
    }

    // 重复投票
    if (voted_servers_.find(resp.get_src()) != voted_servers_.end())
    {
        l_->info(sstrfmt("Duplicate vote from %d for term %lld").fmt(resp.get_src(), state_->get_term()));
        return;
    }

    {
        read_lock(peers_lock_);
        voted_servers_.insert(resp.get_src());
        if (resp.get_accepted())
        {
            votes_granted_ += 1;
        }

        if (voted_servers_.size() >= (peers_.size() + 1))
        {
            election_completed_ = true;
        }

        if (votes_granted_ > (int32)((peers_.size() + 1) / 2))
        {
            l_->info(sstrfmt("Server is elected as leader for term %llu").fmt(state_->get_term()));
            election_completed_ = true;
            become_leader();
        }
    }
}

void raft_server::handle_prevote_resp(resp_msg& resp)
{
    if (!prevote_state_)
    {
        l_->info(sstrfmt("Prevote has completed, term received: %llu, current term %llu")
                     .fmt(resp.get_term(), state_->get_term()));
        return;
    }

    {
        read_lock(peers_lock_);
        bool vote_added = prevote_state_->add_voted_server(resp.get_src());
        if (!vote_added)
        {
            l_->info("Prevote has from %d has been processed.");
            return;
        }

        if (resp.get_accepted())
        {
            prevote_state_->inc_accepted_votes();
        }

        if (prevote_state_->get_accepted_votes() > (int32)((peers_.size() + 1) / 2))
        {
            l_->info(sstrfmt("Prevote passed for term %llu").fmt(state_->get_term()));
            become_candidate();
        }
        else if (prevote_state_->num_of_votes() >= (peers_.size() + 1))
        {
            l_->info(sstrfmt("Prevote failed for term %llu").fmt(state_->get_term()));
            prevote_state_->reset();  // still in prevote state, just reset the prevote state
            restart_election_timer(); // restart election timer for a new round of prevote
        }
    }
}

// 部分request的回调绑定
void raft_server::handle_ext_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err)
{
    recur_lock(lock_);
    if (err)
    {
        handle_ext_resp_err(*err);
        return;
    }

    l_->debug(lstrfmt("Receive an extended %s message from peer %d with Result=%d, Term=%llu, NextIndex=%llu")
                  .fmt(
                      __msg_type_str[resp->get_type()],
                      resp->get_src(),
                      resp->get_accepted() ? 1 : 0,
                      resp->get_term(),
                      resp->get_next_idx()));

    switch (resp->get_type())
    {
        case msg_type::sync_log_response:
            if (srv_to_join_)
            {
                // we are reusing heartbeat interval value to indicate when to stop retry
                srv_to_join_->resume_hb_speed();
                srv_to_join_->set_next_log_idx(resp->get_next_idx());
                srv_to_join_->set_matched_idx(resp->get_next_idx() - 1);
                sync_log_to_new_srv(resp->get_next_idx());
            }
            break;
        case msg_type::join_cluster_response:
            if (srv_to_join_)
            {
                if (resp->get_accepted())
                {
                    l_->debug("new server confirms it will join, start syncing logs to it");
                    sync_log_to_new_srv(resp->get_next_idx());
                }
                else
                {
                    l_->debug("new server cannot accept the invitation, give up");
                }
            }
            else
            {
                l_->debug("no server to join, drop the message");
            }
            break;
        case msg_type::leave_cluster_response:
            if (!resp->get_accepted())
            {
                l_->debug("peer doesn't accept to stepping down, stop proceeding");
                return;
            }

            l_->debug("peer accepted to stepping down, removing this server from cluster");
            // 将离开的节点移除
            rm_srv_from_cluster(resp->get_src());
            break;
        case msg_type::install_snapshot_response:
        {
            if (!srv_to_join_)
            {
                l_->info("no server to join, the response must be very old.");
                return;
            }

            if (!resp->get_accepted())
            {
                l_->info("peer doesn't accept the snapshot installation request");
                return;
            }

            ptr<snapshot_sync_ctx> sync_ctx = srv_to_join_->get_snapshot_sync_ctx();
            if (sync_ctx == nilptr)
            {
                l_->err("Bug! SnapshotSyncContext must not be null");
                ctx_->state_mgr_->system_exit(-1);
                ::exit(-1);
                return;
            }

            if (resp->get_next_idx() >= sync_ctx->get_snapshot()->size())
            {
                // snapshot is done
                ptr<snapshot> nil_snap;
                l_->debug("snapshot has been copied and applied to new server, continue to sync logs after snapshot");
                srv_to_join_->set_snapshot_in_sync(nil_snap);
                srv_to_join_->set_next_log_idx(sync_ctx->get_snapshot()->get_last_log_idx() + 1);
                srv_to_join_->set_matched_idx(sync_ctx->get_snapshot()->get_last_log_idx());
            }
            else
            {
                sync_ctx->set_offset(resp->get_next_idx());
                l_->debug(sstrfmt("continue to send snapshot to new server at offset %llu").fmt(resp->get_next_idx()));
            }

            sync_log_to_new_srv(srv_to_join_->get_next_log_idx());
        }
        break;
        case msg_type::prevote_response:
            handle_prevote_resp(*resp);
            break;
        default:
            l_->err(lstrfmt("received an unexpected response message type %s, for safety, stepping down")
                        .fmt(__msg_type_str[resp->get_type()]));
            ctx_->state_mgr_->system_exit(-1);
            ::exit(-1);
            break;
    }
}

void raft_server::handle_ext_resp_err(rpc_exception& err)
{
    l_->debug(lstrfmt("receive an rpc error response from peer server, %s").fmt(err.what()));
    ptr<req_msg> req = err.req();
    if (req->get_type() == msg_type::sync_log_request || req->get_type() == msg_type::join_cluster_request ||
        req->get_type() == msg_type::leave_cluster_request)
    {
        ptr<peer> p;
        if (req->get_type() == msg_type::leave_cluster_request)
        {
            read_lock(peers_lock_);
            peer_itor pit = peers_.find(req->get_dst());
            if (pit != peers_.end())
            {
                p = pit->second;
            }
        }
        else
        {
            p = srv_to_join_;
        }

        if (p != nilptr)
        {
            if (p->get_current_hb_interval() >= ctx_->params_->max_hb_interval())
            {
                if (req->get_type() == msg_type::leave_cluster_request)
                {
                    l_->info(lstrfmt("rpc failed again for the removing server (%d), will remove this server directly")
                                 .fmt(p->get_id()));

                    /**
                     * In case of there are only two servers in the cluster, it safe to remove the server directly from
                     * peers as at most one config change could happen at a time prove: assume there could be two config
                     * changes at a time this means there must be a leader after previous leader offline, which is
                     * impossible (no leader could be elected after one server goes offline in case of only two servers
                     * in a cluster) so the bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E does not
                     * apply to cluster which only has two members
                     */
                    {
                        write_lock(peers_lock_);
                        if (peers_.size() == 1)
                        {
                            peer_itor pit = peers_.find(p->get_id());
                            if (pit != peers_.end())
                            {
                                pit->second->enable_hb(false);
                                peers_.erase(pit);
                                l_->info(sstrfmt("server %d is removed from cluster").fmt(p->get_id()));
                            }
                            else
                            {
                                l_->info(sstrfmt("peer %d cannot be found, no action for removing").fmt(p->get_id()));
                            }
                        }
                    }

                    rm_srv_from_cluster(p->get_id());
                }
                else
                {
                    l_->info(lstrfmt("rpc failed again for the new coming server (%d), will stop retry for this server")
                                 .fmt(p->get_id()));
                    config_changing_ = false;
                    srv_to_join_.reset();
                }
            }
            else
            {
                // reuse the heartbeat interval value to indicate when to stop retrying, as rpc backoff is the same
                l_->debug("retry the request");
                p->slow_down_hb();
                timer_task<void>::executor exec = [this, p, req]() mutable { this->on_retryable_req_err(p, req); };
                ptr<delayed_task> task(cs_new<timer_task<void>>(exec));
                scheduler_->schedule(task, p->get_current_hb_interval());
            }
        }
    }
}