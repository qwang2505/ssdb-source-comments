/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "sorted_set.h"

int SortedSet::size() const{
	return (int)sorted_set.size();
}

int SortedSet::add(const std::string &key, int64_t score){
	int ret;
	std::map<std::string, std::set<Item>::iterator>::iterator it;
	
	// 先从map里找，会快一点
	it = existed.find(key);
	if(it == existed.end()){
		// new item
		// 没找到，是新的元素
		ret = 1;
	}else{
		// 找到了，key存在
		ret = 0;
		std::set<Item>::iterator it2 = it->second;
		const Item &item = *it2;
		// 查看分数是否相同。如果相同，什么都不许要做，否则先把元素删除
		if(item.score == score){
			// not updated
			return 0;
		}
		// remove existing item
		sorted_set.erase(it2);
	}
	
	Item item;
	item.key = key;
	item.score = score;
	
	// 插入到zset里
	std::pair<std::set<Item>::iterator, bool> p = sorted_set.insert(item);
	// 更新map里的元素分数分数
	existed[key] = p.first;
	
	return ret;
}

int SortedSet::del(const std::string &key){
	int ret;
	std::map<std::string, std::set<Item>::iterator>::iterator it;
	
	// 先在map里查找
	it = existed.find(key);
	if(it == existed.end()){
		// new item
		// 没找到就不用删除了
		ret = 0;
	}else{
		ret = 1;
		// 找到了，从zset和map中删除
		sorted_set.erase(it->second);
		existed.erase(it);
	}
	return ret;
}

int SortedSet::front(std::string *key, int64_t *score) const{
	std::set<Item>::iterator it2 = sorted_set.begin();
	if(it2 == sorted_set.end()){
		// 空set
		return 0;
	}
	// 拿到第一个元素，把key和value放到传入的指针参数里
	const Item &item = *it2;
	*key = item.key;
	// 为什么score加判断，key不用？
	// 因为score是可选参数，有时候不关心分数，只要key
	if(score){
		*score = item.score;
	}
	return 1;
}

int SortedSet::back(std::string *key, int64_t *score) const{
	// 和front一样，就是改成拿最后一个
	std::set<Item>::reverse_iterator it2 = sorted_set.rbegin();
	if(it2 == sorted_set.rend()){
		return 0;
	}
	const Item &item = *it2;
	*key = item.key;
	if(score){
		*score = item.score;
	}
	return 1;
}

int64_t SortedSet::max_score() const{
	int64_t score = 0;
	std::string key;
	// 感谢std::set，最后一个就是最大的
	this->back(&key, &score);
	return score;
}


int SortedSet::pop_front(){
	// 取出第一个，并且从set里删除
	if(sorted_set.empty()){
		return 0;
	}
	std::set<Item>::iterator it = sorted_set.begin();
	const Item &item = *it;
	existed.erase(item.key);
	sorted_set.erase(it);
	return 1;
}

int SortedSet::pop_back(){
	if(sorted_set.empty()){
		return 0;
	}
	// 取出最后一个，并且从set里删除
	std::set<Item>::iterator it = sorted_set.end();
	it --;
	const Item &item = *it;
	existed.erase(item.key);
	sorted_set.erase(it);
	return 1;
}
