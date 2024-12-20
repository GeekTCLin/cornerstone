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

/**
 * 处理RPC 请求
 * append_entries_request || vote_request || install_snapshot_request
 * 需要先进行term 比较，即调用 update_term
 * 
 * follower     接收    append_entries_request  req term 更大 更新term，同时重新开始超时选举
 * candidate    接收    append_entries_request  req term 更大或者 term 相同，变成 follower  
 */
ptr<resp_msg> raft_server::process_req(req_msg& req)
{
    ptr<resp_msg> resp;
    l_->debug(lstrfmt("Receive a %s message from %d with LastLogIndex=%llu, LastLogTerm=%llu, EntriesLength=%d, "
                      "CommitIndex=%llu and Term=%llu")
                  .fmt(
                      __msg_type_str[req.get_type()],
                      req.get_src(),
                      req.get_last_log_idx(),
                      req.get_last_log_term(),
                      req.log_entries().size(),
                      req.get_commit_idx(),
                      req.get_term()));
    {
        recur_lock(lock_);
        if (req.get_type() == msg_type::append_entries_request || req.get_type() == msg_type::vote_request ||
            req.get_type() == msg_type::install_snapshot_request)
        {
            // we allow the server to be continue after term updated to save a round message
            // 更新任期
            // vote_request 会主动直接更新任期
            update_term(req.get_term());

            // Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
            if (steps_to_down_ > 0)
            {
                steps_to_down_ = 2;
            }
        }

        if (req.get_type() == msg_type::append_entries_request)
        {
            resp = handle_append_entries(req);
        }
        else if (req.get_type() == msg_type::vote_request)
        {
            resp = handle_vote_req(req);
        }
        else if (req.get_type() == msg_type::client_request)
        {
            resp = handle_cli_req(req);
        }
        else
        {
            // extended requests
            resp = handle_extended_msg(req);
        }
    }

    if (resp)
    {
        l_->debug(lstrfmt("Response back a %s message to %d with Accepted=%d, Term=%llu, NextIndex=%llu")
                      .fmt(
                          __msg_type_str[resp->get_type()],
                          resp->get_dst(),
                          resp->get_accepted() ? 1 : 0,
                          resp->get_term(),
                          resp->get_next_idx()));
    }

    return resp;
}

ptr<resp_msg> raft_server::handle_append_entries(req_msg& req)
{
    if (req.get_term() == state_->get_term())
    {
        if (role_ == srv_role::candidate)
        {
            become_follower();
        }
        else if (role_ == srv_role::leader)
        {
            l_->debug(lstrfmt("Receive AppendEntriesRequest from another leader(%d) with same term, there must be a "
                              "bug, server exits")
                          .fmt(req.get_src()));
            ctx_->state_mgr_->system_exit(-1);
            ::exit(-1);
        }
        else
        {
            restart_election_timer();
        }
    }

    // After a snapshot the req.get_last_log_idx() may less than log_store_->next_slot() but equals to
    // log_store_->next_slot() -1 In this case, log is Okay if req.get_last_log_idx() == lastSnapshot.get_last_log_idx()
    // && req.get_last_log_term() == lastSnapshot.get_last_log_term() In not accepted case, we will return
    // log_store_->next_slot() for the leader to quick jump to the index that might aligned
    ptr<resp_msg> resp(cs_new<resp_msg>(
        state_->get_term(), msg_type::append_entries_response, id_, req.get_src(), log_store_->next_slot()));

    // 注意这里的 req.get_last_log_idx() last_log_index 是 peer 对象记录的，不是 leader 节点log_store 的last_log_index
    // 理论上来说，上次同步的 log_idx 肯定 小于 next_slot ，反之发来的日志 大于期待接收的idx，需要回滚判断
    bool log_okay = req.get_last_log_idx() == 0 || (req.get_last_log_idx() < log_store_->next_slot() &&
                                                    req.get_last_log_term() == term_for_log(req.get_last_log_idx()));
    bool term_okey = req.get_term() == state_->get_term();
    
    //if (req.get_term() < state_->get_term() || !log_okay)
    if(!term_okey || !log_okay)
    {
        return resp;
    }

    // follower & log is okay
    if (req.log_entries().size() > 0)
    {
        // write logs to store, start from overlapped logs
        // 过滤相同的日志
        ulong idx = req.get_last_log_idx() + 1;
        size_t log_vector_idx = 0;
        while (idx < log_store_->next_slot() && log_vector_idx < req.log_entries().size())
        {
            if (log_store_->term_at(idx) == req.log_entries().at(log_vector_idx)->get_term())
            {
                idx++;
                log_vector_idx++;
            }
            else
            {
                break;
            }
        }

        // dealing with overwrites
        // 重写日志
        while (idx < log_store_->next_slot() && log_vector_idx < req.log_entries().size())
        {
            ptr<log_entry> old_entry(log_store_->entry_at(idx));
            if (old_entry->get_val_type() == log_val_type::app_log)
            {
                // 状态机回滚
                state_machine_->rollback(idx, old_entry->get_buf(), old_entry->get_cookie());
            }
            else if (old_entry->get_val_type() == log_val_type::conf)
            {
                l_->info(sstrfmt("revert from a prev config change to config at %llu").fmt(config_->get_log_idx()));
                config_changing_ = false;
            }

            ptr<log_entry> entry = req.log_entries().at(log_vector_idx);
            log_store_->write_at(idx, entry);
            if (entry->get_val_type() == log_val_type::app_log)
            {
                state_machine_->pre_commit(idx, entry->get_buf(), entry->get_cookie());
            }
            else if (entry->get_val_type() == log_val_type::conf)
            {
                l_->info(sstrfmt("receive a config change from leader at %llu").fmt(idx));
                config_changing_ = true;
            }

            idx += 1;
            log_vector_idx += 1;
        }

        // append new log entries
        // 添加新日志
        while (log_vector_idx < req.log_entries().size())
        {
            ptr<log_entry> entry = req.log_entries().at(log_vector_idx++);
            ulong idx_for_entry = log_store_->append(entry);
            if (entry->get_val_type() == log_val_type::conf)
            {
                l_->info(sstrfmt("receive a config change from leader at %llu").fmt(idx_for_entry));
                config_changing_ = true;
            }
            else if (entry->get_val_type() == log_val_type::app_log)
            {
                state_machine_->pre_commit(idx_for_entry, entry->get_buf(), entry->get_cookie());
            }
        }
    }

    leader_ = req.get_src();
    // 同步commit
    commit(req.get_commit_idx());
    // 设置下一个期待日志idx
    resp->accept(req.get_last_log_idx() + req.log_entries().size() + 1);
    return resp;
}

// 处理投票请求
ptr<resp_msg> raft_server::handle_vote_req(req_msg& req)
{
    // 注意这里 req 发来的 last_log_term 和 last_log_idx 为 candidate节点 log_store 数据
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::vote_response, id_, req.get_src()));
    bool log_okay = req.get_last_log_term() > log_store_->last_entry()->get_term() ||
                    (req.get_last_log_term() == log_store_->last_entry()->get_term() &&
                     log_store_->next_slot() - 1 <= req.get_last_log_idx());
    bool grant = req.get_term() == state_->get_term() && log_okay &&
                 (state_->get_voted_for() == req.get_src() || state_->get_voted_for() == -1);
    if (grant)
    {
        resp->accept(log_store_->next_slot());
        // 成功投票
        state_->set_voted_for(req.get_src());
        ctx_->state_mgr_->save_state(*state_);
    }

    return resp;
}

// 处理预提交请求
ptr<resp_msg> raft_server::handle_prevote_req(req_msg& req)
{
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::prevote_response, id_, req.get_src()));
    bool log_okay = req.get_last_log_term() > log_store_->last_entry()->get_term() ||
                    (req.get_last_log_term() == log_store_->last_entry()->get_term() &&
                     log_store_->next_slot() - 1 <= req.get_last_log_idx());
    // 预投票 任期还没增加，所以 >= 即可
    bool grant = req.get_term() >= state_->get_term() && log_okay;
    if (ctx_->params_->defensive_prevote_)
    {
        // In defensive mode, server will deny the prevote when it's operating well.
        grant = grant && prevote_state_;
    }

    if (grant)
    {
        resp->accept(log_store_->next_slot());
    }

    return resp;
}

ptr<resp_msg> raft_server::handle_cli_req(req_msg& req)
{
    bool leader = is_leader();

    // check if leader has expired.
    // there could be a case that the leader just elected, in that case, client can
    // just simply retry, no safety issue here.
    if (role_ == srv_role::leader && !leader)
    {
        // leader 过期了
        return cs_new<resp_msg>(state_->get_term(), msg_type::append_entries_response, id_, -1);
    }
    
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::append_entries_response, id_, leader_));
    if (!leader)
    {
        // 非leader 节点
        return resp;
    }

    std::vector<ptr<log_entry>>& entries = req.log_entries();
    for (size_t i = 0; i < entries.size(); ++i)
    {
        // force the log's term to current term
        entries.at(i)->set_term(state_->get_term());

        log_store_->append(entries.at(i));
        // 状态机 预提交（提供的接口，可不实现）
        state_machine_->pre_commit(log_store_->next_slot() - 1, entries.at(i)->get_buf(), entries.at(i)->get_cookie());
    }

    // urgent commit, so that the commit will not depend on hb
    request_append_entries();
    resp->accept(log_store_->next_slot());
    return resp;
}

ptr<resp_msg> raft_server::handle_extended_msg(req_msg& req)
{
    switch (req.get_type())
    {
        case msg_type::add_server_request:
            return handle_add_srv_req(req);
        case msg_type::remove_server_request:
            return handle_rm_srv_req(req);
        case msg_type::sync_log_request:
            return handle_log_sync_req(req);
        case msg_type::join_cluster_request:
            return handle_join_cluster_req(req);
        case msg_type::leave_cluster_request:
            return handle_leave_cluster_req(req);
        case msg_type::install_snapshot_request:
            return handle_install_snapshot_req(req);
        case msg_type::prevote_request:
            return handle_prevote_req(req);
        default:
            l_->err(
                sstrfmt("receive an unknown request %s, for safety, step down.").fmt(__msg_type_str[req.get_type()]));
            ctx_->state_mgr_->system_exit(-1);
            ::exit(-1);
            break;
    }

    return ptr<resp_msg>();
}

ptr<resp_msg> raft_server::handle_install_snapshot_req(req_msg& req)
{
    if (req.get_term() == state_->get_term() && !catching_up_)
    {
        if (role_ == srv_role::candidate)
        {
            // 如果一个节点脱离集群，超时后增加任期可能会成为candidate
            // 而leader节点因为一直没脸上该节点，日志长期未同步则发送了快照同步请求
            become_follower();
        }
        else if (role_ == srv_role::leader)
        {
            l_->err(lstrfmt("Receive InstallSnapshotRequest from another leader(%d) with same term, there must be a "
                            "bug, server exits")
                        .fmt(req.get_src()));
            ctx_->state_mgr_->system_exit(-1);
            ::exit(-1);
            return ptr<resp_msg>();
        }
        else
        {
            restart_election_timer();
        }
    }

    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::install_snapshot_response, id_, req.get_src()));
    if (!catching_up_ && req.get_term() < state_->get_term())
    {
        l_->info("received an install snapshot request which has lower term than this server, decline the request");
        return resp;
    }

    std::vector<ptr<log_entry>>& entries(req.log_entries());
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::snp_sync_req)
    {
        l_->warn("Receive an invalid InstallSnapshotRequest due to bad log entries or bad log entry value");
        return resp;
    }

    // 从 req 中取出 sync_req
    ptr<snapshot_sync_req> sync_req(snapshot_sync_req::deserialize(entries[0]->get_buf()));
    if (sync_req->get_snapshot().get_last_log_idx() <= sm_commit_index_)
    {
        l_->warn(sstrfmt("received a snapshot (%llu) that is older than current log store")
                     .fmt(sync_req->get_snapshot().get_last_log_idx()));
        return resp;
    }

    if (handle_snapshot_sync_req(*sync_req))
    {
        // 注意这里的计算， next_idx 是下一个期待的字节数下标了
        resp->accept(sync_req->get_offset() + sync_req->get_data().size());
    }

    return resp;
}

bool raft_server::handle_snapshot_sync_req(snapshot_sync_req& req)
{
    try
    {
        // 本地状态机（存储引擎）写入 部分快照数据
        state_machine_->save_snapshot_data(req.get_snapshot(), req.get_offset(), req.get_data());
        if (req.is_done())
        {
            // Only follower will run this piece of code, but let's check it again
            if (role_ != srv_role::follower)
            {
                l_->err("bad server role for applying a snapshot, exit for debugging");
                ctx_->state_mgr_->system_exit(-1);
                ::exit(-1);
            }

            l_->debug("sucessfully receive a snapshot from leader");
            if (log_store_->compact(req.get_snapshot().get_last_log_idx()))
            {
                // The state machine will not be able to commit anything before the snapshot is applied, so make this
                // synchronously with election timer stopped as usually applying a snapshot may take a very long time
                stop_election_timer();
                l_->info("successfully compact the log store, will now ask the statemachine to apply the snapshot");
                if (!state_machine_->apply_snapshot(req.get_snapshot()))
                {
                    l_->info("failed to apply the snapshot after log compacted, to ensure the safety, will shutdown "
                             "the system");
                    ctx_->state_mgr_->system_exit(-1);
                    ::exit(-1);
                    return false;
                }

                reconfigure(req.get_snapshot().get_last_config());
                ctx_->state_mgr_->save_config(*config_);
                sm_commit_index_ = req.get_snapshot().get_last_log_idx();
                quick_commit_idx_ = req.get_snapshot().get_last_log_idx();
                ctx_->state_mgr_->save_state(*state_);
                last_snapshot_ = cs_new<snapshot>(
                    req.get_snapshot().get_last_log_idx(),
                    req.get_snapshot().get_last_log_term(),
                    config_,
                    req.get_snapshot().size());
                restart_election_timer();
                l_->info("snapshot is successfully applied");
            }
            else
            {
                l_->err("failed to compact the log store after a snapshot is received, will ask the leader to retry");
                return false;
            }
        }
    }
    catch (...)
    {
        l_->err("failed to handle snapshot installation due to system errors");
        ctx_->state_mgr_->system_exit(-1);
        ::exit(-1);
        return false;
    }

    return true;
}

void raft_server::on_retryable_req_err(ptr<peer>& p, ptr<req_msg>& req)
{
    l_->debug(sstrfmt("retry the request %s for %d").fmt(__msg_type_str[req->get_type()], p->get_id()));
    p->send_req(req, ex_resp_handler_);
}

// leader 通知被移除的 服务器节点
ptr<resp_msg> raft_server::handle_rm_srv_req(req_msg& req)
{
    std::vector<ptr<log_entry>>& entries(req.log_entries());
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::remove_server_response, id_, leader_));
    if (entries.size() != 1 || entries[0]->get_buf().size() != sz_int)
    {
        l_->info("bad remove server request as we are expecting one log entry with value type of int");
        return resp;
    }

    if (role_ != srv_role::leader)
    {
        l_->info("this is not a leader, cannot handle RemoveServerRequest");
        return resp;
    }

    if (config_changing_)
    {
        // the previous config has not committed yet
        l_->info("previous config has not committed yet");
        return resp;
    }

    int32 srv_id = entries[0]->get_buf().get_int();
    if (srv_id == id_)
    {
        l_->info("cannot request to remove leader");
        return resp;
    }

    ptr<peer> p;
    {
        read_lock(peers_lock_);
        peer_itor pit = peers_.find(srv_id);
        if (pit == peers_.end())
        {
            l_->info(sstrfmt("server %d does not exist").fmt(srv_id));
            return resp;
        }

        p = pit->second;
    }

    ptr<req_msg> leave_req(cs_new<req_msg>(
        state_->get_term(),
        msg_type::leave_cluster_request,
        id_,
        srv_id,
        0,
        log_store_->next_slot() - 1,
        quick_commit_idx_));
    p->send_req(leave_req, ex_resp_handler_);
    resp->accept(log_store_->next_slot());
    return resp;
}

// 处理添加节点请求
ptr<resp_msg> raft_server::handle_add_srv_req(req_msg& req)
{
    std::vector<ptr<log_entry>>& entries(req.log_entries());
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::add_server_response, id_, leader_));
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::cluster_server)
    {
        l_->debug("bad add server request as we are expecting one log entry with value type of ClusterServer");
        return resp;
    }

    if (role_ != srv_role::leader)
    {
        l_->info("this is not a leader, cannot handle AddServerRequest");
        return resp;
    }

    // 从 buffer中解析数据
    ptr<srv_config> srv_conf(srv_config::deserialize(entries[0]->get_buf()));
    {
        read_lock(peers_lock_);
        if (peers_.find(srv_conf->get_id()) != peers_.end() || id_ == srv_conf->get_id())
        {
            // id 重复
            l_->warn(
                lstrfmt("the server to be added has a duplicated id with existing server %d").fmt(srv_conf->get_id()));
            return resp;
        }
    }

    if (config_changing_)
    {
        // the previous config has not committed yet
        l_->info("previous config has not committed yet");
        return resp;
    }

    conf_to_add_ = std::move(srv_conf);
    timer_task<peer&>::executor exec = [this](peer& p) { this->handle_hb_timeout(p); };
    srv_to_join_ = cs_new<peer>(conf_to_add_, *ctx_, exec);
    invite_srv_to_join_cluster();
    resp->accept(log_store_->next_slot());
    return resp;
}

ptr<resp_msg> raft_server::handle_log_sync_req(req_msg& req)
{
    std::vector<ptr<log_entry>>& entries = req.log_entries();
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::sync_log_response, id_, req.get_src()));
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::log_pack)
    {
        l_->info("receive an invalid LogSyncRequest as the log entry value doesn't meet the requirements");
        return resp;
    }

    if (!catching_up_)
    {
        l_->info("This server is ready for cluster, ignore the request");
        return resp;
    }

    log_store_->apply_pack(req.get_last_log_idx() + 1, entries[0]->get_buf());
    commit(log_store_->next_slot() - 1);
    resp->accept(log_store_->next_slot());
    return resp;
}

void raft_server::sync_log_to_new_srv(ulong start_idx)
{
    // only sync committed logs
    int32 gap = (int32)(quick_commit_idx_ - start_idx);
    if (gap < ctx_->params_->log_sync_stop_gap_)
    {
        l_->info(lstrfmt("LogSync is done for server %d with log gap %d, now put the server into cluster")
                     .fmt(srv_to_join_->get_id(), gap));
        ptr<cluster_config> new_conf = cs_new<cluster_config>(log_store_->next_slot(), config_->get_log_idx());
        new_conf->get_servers().insert(
            new_conf->get_servers().end(), config_->get_servers().begin(), config_->get_servers().end());
        new_conf->get_servers().push_back(conf_to_add_);
        bufptr new_conf_buf(new_conf->serialize());
        // 同步 新加入节点的配置日志 给peer
        ptr<log_entry> entry(cs_new<log_entry>(state_->get_term(), std::move(new_conf_buf), log_val_type::conf));
        log_store_->append(entry);
        config_changing_ = true;
        request_append_entries();
        return;
    }

    ptr<req_msg> req;
    if (start_idx > 0 && start_idx < log_store_->start_index())
    {
        req = create_sync_snapshot_req(*srv_to_join_, start_idx, state_->get_term(), quick_commit_idx_);
    }
    else
    {
        int32 size_to_sync = std::min(gap, ctx_->params_->log_sync_batch_size_);
        bufptr log_pack = log_store_->pack(start_idx, size_to_sync);
        req = cs_new<req_msg>(
            state_->get_term(),
            msg_type::sync_log_request,
            id_,
            srv_to_join_->get_id(),
            0L,
            start_idx - 1,
            quick_commit_idx_);
        req->log_entries().push_back(
            cs_new<log_entry>(state_->get_term(), std::move(log_pack), log_val_type::log_pack));
    }

    srv_to_join_->send_req(req, ex_resp_handler_);
}

void raft_server::invite_srv_to_join_cluster()
{
    // 发送加入集群请求
    ptr<req_msg> req(cs_new<req_msg>(
        state_->get_term(),
        msg_type::join_cluster_request,
        id_,
        srv_to_join_->get_id(),
        0L,
        log_store_->next_slot() - 1,
        quick_commit_idx_));
    req->log_entries().push_back(cs_new<log_entry>(state_->get_term(), config_->serialize(), log_val_type::conf));
    srv_to_join_->send_req(req, ex_resp_handler_);
}

// 处理加入集群请求
ptr<resp_msg> raft_server::handle_join_cluster_req(req_msg& req)
{
    std::vector<ptr<log_entry>>& entries = req.log_entries();
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::join_cluster_response, id_, req.get_src()));
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::conf)
    {
        l_->info("receive an invalid JoinClusterRequest as the log entry value doesn't meet the requirements");
        return resp;
    }

    if (catching_up_)
    {
        l_->info("this server is already in log syncing mode");
        return resp;
    }

    catching_up_ = true;
    // 设置为follower
    role_ = srv_role::follower;
    // 存储 leader 节点的 id
    leader_ = req.get_src();
    sm_commit_index_ = 0;
    quick_commit_idx_ = 0;
    state_->set_voted_for(-1);
    state_->set_term(req.get_term());
    ctx_->state_mgr_->save_state(*state_);
    reconfigure(cluster_config::deserialize(entries[0]->get_buf()));
    resp->accept(log_store_->next_slot());
    return resp;
}

// 处理离开 集群请求，更新 steps_to_down_，仅这里会修改 steps_to_down_ 大于0
// 看样子是 另外一端发送至此节点进行离开操作
ptr<resp_msg> raft_server::handle_leave_cluster_req(req_msg& req)
{
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::leave_cluster_response, id_, req.get_src()));
    if (!config_changing_)
    {
        steps_to_down_ = 2;
        resp->accept(log_store_->next_slot());
    }

    return resp;
}

void raft_server::rm_srv_from_cluster(int32 srv_id)
{
    // 构建新 cluster_config
    ptr<cluster_config> new_conf = cs_new<cluster_config>(log_store_->next_slot(), config_->get_log_idx());
    for (cluster_config::const_srv_itor it = config_->get_servers().begin(); it != config_->get_servers().end(); ++it)
    {
        if ((*it)->get_id() != srv_id)
        {
            new_conf->get_servers().push_back(*it);
        }
    }

    l_->info(lstrfmt("removed a server from configuration and save the configuration to log store at %llu")
                 .fmt(new_conf->get_log_idx()));
    config_changing_ = true;
    bufptr new_conf_buf(new_conf->serialize());
    // 将新配置写入 日志，同步至其他节点
    ptr<log_entry> entry(cs_new<log_entry>(state_->get_term(), std::move(new_conf_buf), log_val_type::conf));
    log_store_->append(entry);
    request_append_entries();
}