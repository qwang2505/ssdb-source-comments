/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "cluster.h"
#include "ssdb/ssdb.h"
#include "util/log.h"
#include "cluster_store.h"
#include "cluster_migrate.h"

Cluster::Cluster(SSDB *db){
	log_debug("Cluster init");
	this->next_id = 1;
	this->db = db;
	this->store = new ClusterStore(db);
}

Cluster::~Cluster(){
	delete store;
	log_debug("Cluster finalized");
}

// 初始化集群
int Cluster::init(){
    // 从ssdb中加载已经存在的集群节点列表
	int ret = this->store->load_kv_node_list(&kv_node_list);
	if(ret == -1){
		log_error("load_kv_node_list failed!");
		return -1;
	}
	// 更新下一个节点ID值
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		const Node &node = *it;
		if(node.id >= this->next_id){
			this->next_id = node.id + 1;
		}
	}
	return 0;
}

// 添加一个节点到集群中
int Cluster::add_kv_node(const std::string &ip, int port){
    // 这里加锁
	Locking l(&mutex);
	// 创建一个节点，ID自增
	Node node;
	node.id = next_id ++;
	node.ip = ip;
	node.port = port;
	
	// 保存节点信息到SSDB
	if(store->save_kv_node(node) == -1){
		return -1;
	}
	
	// 保存节点信息到内存
	kv_node_list.push_back(node);
	// 返回新增加的节点的ID
	return node.id;
}

// 根据ID删除一个节点
int Cluster::del_kv_node(int id){
	Locking l(&mutex);
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		const Node &node = *it;
		if(node.id == id){
		    // 从SSDB删除
			if(store->del_kv_node(id) == -1){
				return -1;
			}
			// 从内存删除
			kv_node_list.erase(it);
			return 1;
		}
	}
	return 0;
}

// 设置节点的KV区间，并将修改保存到数据库
int Cluster::set_kv_range(int id, const KeyRange &range){
	Locking l(&mutex);
	// 检测给定的区间，确保指定区间和集群中已有节点的区间都没有重叠
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		Node &node = *it;
		if(node.id != id && node.status == Node::SERVING){
			if(node.range.overlapped(range)){
				log_error("range overlapped!");
				return -1;
			}
		}
	}
	
	// 根据ID获取节点
	Node *node = this->get_kv_node_ref(id);
	if(!node){
		return 0;
	}
	// 设置节点的key区间
	node->range = range;
	// 将修改保存到数据库
	if(store->save_kv_node(*node) == -1){
		return -1;
	}
	return 1;
}

// 设置节点的状态
int Cluster::set_kv_status(int id, int status){
	Locking l(&mutex);
	// 根据ID获取节点
	Node *node = this->get_kv_node_ref(id);
	if(!node){
		return 0;
	}
	// 修改节点状态并保存
	node->status = status;
	if(store->save_kv_node(*node) == -1){
		return -1;
	}
	return 1;
}

// 获取节点列表
int Cluster::get_kv_node_list(std::vector<Node> *list){
	Locking l(&mutex);
	*list = kv_node_list;
	return 0;
}

// 根据ID获取节点的指针
Node* Cluster::get_kv_node_ref(int id){
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		Node &node = *it;
		if(node.id == id){
			return &node;
		}
	}
	return NULL;
}

// 根据ID获取节点
int Cluster::get_kv_node(int id, Node *ret){
	Locking l(&mutex);
	Node *node = this->get_kv_node_ref(id);
	if(node){
		*ret = *node;
		return 1;
	}
	return 0;
}

// 从一个节点向另一个节点迁移数据
int64_t Cluster::migrate_kv_data(int src_id, int dst_id, int num_keys){
	Locking l(&mutex);
	
	// 根据ID获取到两个节点
	Node *src = this->get_kv_node_ref(src_id);
	if(!src){
		return -1;
	}
	Node *dst = this->get_kv_node_ref(dst_id);
	if(!dst){
		return -1;
	}
	
	ClusterMigrate migrate;
	// 从一个节点向另一个节点迁移数据，过程中会改变src和dst节点的key区间
	int64_t size = migrate.migrate_kv_data(src, dst, num_keys);
	if(size > 0){
	    // 将修改后的key区间保存
		if(store->save_kv_node(*src) == -1){
			log_error("after migrate_kv_data, save src failed!");
			return -1;
		}
		if(store->save_kv_node(*dst) == -1){
			log_error("after migrate_kv_data, save dst failed!");
			return -1;
		}
	}
	return size;
}

