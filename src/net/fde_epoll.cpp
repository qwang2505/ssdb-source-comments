/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_FDE_EPOLL_H
#define UTIL_FDE_EPOLL_H

Fdevents::Fdevents(){
    // 创建epoll文件描述符，注意和事件的文件描述符区分开
	ep_fd = epoll_create(1024);
}

Fdevents::~Fdevents(){
    // 删除事件
	for(int i=0; i<(int)events.size(); i++){
		delete events[i];
	}
	// 关闭文件描述符
	if(ep_fd){
		::close(ep_fd);
	}
	// 清空事件列表
	events.clear();
	ready_events.clear();
}

// 查看某个文件描述符的某个flag是否被设置
bool Fdevents::isset(int fd, int flag){
	struct Fdevent *fde = get_fde(fd);
	return (bool)(fde->s_flags & flag);
}

// 给某个文件描述符设置flag以及添加数据，并开始监听fd的某些事件
int Fdevents::set(int fd, int flags, int data_num, void *data_ptr){
	struct Fdevent *fde = get_fde(fd);
	// 已经设置了
	if(fde->s_flags & flags){
		return 0;
	}
	// 控制操作符，如果已经存在就是修改，否则就是添加
	int ctl_op = fde->s_flags? EPOLL_CTL_MOD : EPOLL_CTL_ADD;

    // 设置flag以及数据
	fde->s_flags |= flags;
	fde->data.num = data_num;
	fde->data.ptr = data_ptr;

    // 创建一个epoll event
	struct epoll_event epe;
	// epoll event的数据是我们自己的event结构的指针
	epe.data.ptr = fde;
	// 这是什么意思？初始化要监听的事件为空
	epe.events = 0;
	// 设置epoll event的flag，也就是设置要监听的事件
	if(fde->s_flags & FDEVENT_IN)  epe.events |= EPOLLIN;
	if(fde->s_flags & FDEVENT_OUT) epe.events |= EPOLLOUT;

    // 这是在修改？这是在注册事件，说明我们要求epoll监听在文件描述符fd的某个事件
	int ret = epoll_ctl(ep_fd, ctl_op, fd, &epe);
	if(ret == -1){
		return -1;
	}
	return 0;
}

// 取消对文件描述符fd的相关事件的监听
int Fdevents::del(int fd){
	struct epoll_event epe;
	int ret = epoll_ctl(ep_fd, EPOLL_CTL_DEL, fd, &epe);
	if(ret == -1){
		return -1;
	}

	struct Fdevent *fde = get_fde(fd);
	fde->s_flags = FDEVENT_NONE;
	return 0;
}

// 取消监听文件描述符fd的某些事件
int Fdevents::clr(int fd, int flags){
	struct Fdevent *fde = get_fde(fd);
	// 本来就没有在监听
	if(!(fde->s_flags & flags)){
		return 0;
	}

	fde->s_flags &= ~flags;
	int ctl_op = fde->s_flags? EPOLL_CTL_MOD: EPOLL_CTL_DEL;

	struct epoll_event epe;
	epe.data.ptr = fde;
	epe.events = 0;
	if(fde->s_flags & FDEVENT_IN)  epe.events |= EPOLLIN;
	if(fde->s_flags & FDEVENT_OUT) epe.events |= EPOLLOUT;

    // 修改监听控制
	int ret = epoll_ctl(ep_fd, ctl_op, fd, &epe);
	if(ret == -1){
		return -1;
	}
	return 0;
}

// 等待事件发生并返回
const Fdevents::events_t* Fdevents::wait(int timeout_ms){
	struct Fdevent *fde;
	struct epoll_event *epe;
	// 清空已就绪事件列表
	ready_events.clear();

    // 等待epoll返回事件，将返回的事件放到ep_events中
	int nfds = epoll_wait(ep_fd, ep_events, MAX_FDS, timeout_ms);
	// 没有事件返回
	if(nfds == -1){
	    // 这是个什么错误？
		if(errno == EINTR){
			return &ready_events;
		}
		return NULL;
	}

    // epoll返回了事件，逐一处理
	for(int i = 0; i < nfds; i++){
		epe = &ep_events[i];
		// 这里很重要：从epoll返回的事件中获取到我们自己设置的事件数据
		// 需要结合具体使用情况看这里是怎么操作的
		fde = (struct Fdevent *)epe->data.ptr;

		fde->events = FDEVENT_NONE;
		if(epe->events & EPOLLIN)  fde->events |= FDEVENT_IN;
		if(epe->events & EPOLLPRI) fde->events |= FDEVENT_IN;
		if(epe->events & EPOLLOUT) fde->events |= FDEVENT_OUT;
		if(epe->events & EPOLLHUP) fde->events |= FDEVENT_ERR;
		if(epe->events & EPOLLERR) fde->events |= FDEVENT_ERR;

		ready_events.push_back(fde);
	}
	return &ready_events;
}

#endif

