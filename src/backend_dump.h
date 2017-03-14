/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_BACKEND_DUMP_H_
#define SSDB_BACKEND_DUMP_H_

#include "include.h"
#include "ssdb/ssdb.h"
#include "net/link.h"

// 貌似是用来做对整个数据库的dump，待查看
// 在命令文档中没有提到dump命令，但是在命令处理中确实有这个命令，可能是文档删掉了？
class BackendDump{
private:
    // 在线程中运行dump时的运行参数
	struct run_arg{
		const Link *link;
		const BackendDump *backend;
	};
	static void* _run_thread(void *arg);
	SSDB *ssdb;
public:
	BackendDump(SSDB *ssdb);
	~BackendDump();
	void proc(const Link *link);
};

#endif
