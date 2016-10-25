/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_THREAD_H_
#define UTIL_THREAD_H_

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <queue>
#include <vector>

// 对线程锁的封装
class Mutex{
	private:
		pthread_mutex_t mutex;
	public:
		Mutex(){
			pthread_mutex_init(&mutex, NULL);
		}
		~Mutex(){
			pthread_mutex_destroy(&mutex);
		}
		void lock(){
			pthread_mutex_lock(&mutex);
		}
		void unlock(){
			pthread_mutex_unlock(&mutex);
		}
};

// 这也是锁，不知道在哪里用到，用到再具体看
class Locking{
	private:
		Mutex *mutex;
		// No copying allowed
		Locking(const Locking&);
		void operator=(const Locking&);
	public:
		Locking(Mutex *mutex){
			this->mutex = mutex;
			this->mutex->lock();
		}
		~Locking(){
			this->mutex->unlock();
		}

};

/*
class Semaphore {
	private:
		pthread_cond_t cond;
		pthread_mutex_t mutex;
	public:
		Semaphore(Mutex* mu){
			pthread_cond_init(&cond, NULL);
			pthread_mutex_init(&mutex, NULL);
		}
		~CondVar(){
			pthread_cond_destroy(&cond);
			pthread_mutex_destroy(&mutex);
		}
		void wait();
		void signal();
};
*/


// 线程安全的队列
// Thread safe queue
template <class T>
class Queue{
	private:
	    // 这里为什么要用条件？不能直接使用锁来控制？
		pthread_cond_t cond;
		pthread_mutex_t mutex;
		std::queue<T> items;
	public:
		Queue();
		~Queue();

		bool empty();
		int size();
		int push(const T item);
		// TODO: with timeout
		int pop(T *data);
};


// Selectable queue, multi writers, single reader
// 为什么是多个写？写之前有加锁，同样不可以多个写吧？
// 和Queue有什么区别？都是线程安全的，只不过Queue用cond
// 来进行控制，SelectableQueue用管道来控制
// NOTICE 注意模板类的用法，要熟悉，且以后需要自己写模板类
template <class T>
class SelectableQueue{
	private:
		int fds[2];
		pthread_mutex_t mutex;
		std::queue<T> items;
	public:
		SelectableQueue();
		~SelectableQueue();
		int fd(){
			return fds[0];
		}

		// multi writer
		int push(const T item);
		// single reader
		int pop(T *data);
};

/**
 * 工作池？其中可以包含很多worker，应该是这样吧
 * 两个模板类，W是worker类，JOB是任务类
 *
 * 这是个多线程的运行某个任务的工具，可以指定线程数量。运行步骤如下：
 * 1. 确定W类和JOB类。也就是，确定worker类和任务类，任务类中实际上包含的应该是任务数据。
 *   这时候worker类可能还没有定义；
 * 2. 定义worker类，需要继承WorkerPool::Worder类，重载proc方法，指定处理任务的逻辑。
 * 3. 创建WorkerPool对象，这时候需要指定Worker类和JOB类。
 * 4. 调用pool的start方法，开始运行，这时候需要指定worker的数量，也就是线程的数量；
 * 5. 向pool中push任务；
 * 6. 从pool中读取任务结果；
 * 7. 调用pool的stop方法，停止运行；
 * TODO 确定在哪里使用的，怎么使用的，再回来好好理解这里的代码
 */
template<class W, class JOB>
class WorkerPool{
	public:
		class Worker{
			public:
				Worker(){};
				Worker(const std::string &name);
				virtual ~Worker(){}
				int id;
				virtual void init(){}
				virtual void destroy(){}
				virtual int proc(JOB *job) = 0;
			private:
			protected:
				std::string name;
		};
	private:
	    // worker pool的名称还是worker的名称？
		std::string name;
		// 任务队列
		Queue<JOB> jobs;
		// 任务处理的结果队列
		SelectableQueue<JOB> results;

        // worker数量
		int num_workers;
		// 线程id列表，应该是worker线程的列表的，那意味着每个worker会在独立的线程中运行?
		std::vector<pthread_t> tids;
		// 是否已开始运行？
		bool started;

        // 运行参数
		struct run_arg{
			int id;
			WorkerPool *tp;
		};
		static void* _run_worker(void *arg);
	public:
		WorkerPool(const char *name="");
		~WorkerPool();

        // 这里返回的是结果队列中用于读取的文件描述符
		int fd(){
			return results.fd();
		}
		
		int start(int num_workers);
		int stop();
		
		int push(JOB job);
		int pop(JOB *job);
};





template <class T>
Queue<T>::Queue(){
    // 初始化线程等待条件
	pthread_cond_init(&cond, NULL);
	// 初始化线程锁
	pthread_mutex_init(&mutex, NULL);
}

template <class T>
Queue<T>::~Queue(){
    // 销毁资源
	pthread_cond_destroy(&cond);
	pthread_mutex_destroy(&mutex);
}

// 判断队列是否空
template <class T>
bool Queue<T>::empty(){
	bool ret = false;
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	ret = items.empty();
	pthread_mutex_unlock(&mutex);
	return ret;
}

// 获取队列长度
template <class T>
int Queue<T>::size(){
	int ret = -1;
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	ret = items.size();
	pthread_mutex_unlock(&mutex);
	return ret;
}

// 向队列添加一个item
template <class T>
int Queue<T>::push(const T item){
    // 为什么这里不等待cond？
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	{
		items.push(item);
	}
	pthread_mutex_unlock(&mutex);
	// 发送condition signal
	pthread_cond_signal(&cond);
	return 1;
}

// 从队列获取内容。如果队列为空，会等待cond发生再去获取内容
template <class T>
int Queue<T>::pop(T *data){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	{
		// 必须放在循环中, 因为 pthread_cond_wait 可能抢不到锁而被其它处理了
		while(items.empty()){
			//fprintf(stderr, "%d wait\n", pthread_self());
			//这里在wait的时候会解锁，这样别的线程就可以访问以及修改数据了
			if(pthread_cond_wait(&cond, &mutex) != 0){
				//fprintf(stderr, "%s %d -1!\n", __FILE__, __LINE__);
				return -1;
			}
			//fprintf(stderr, "%d wait 2\n", pthread_self());
		}
		*data = items.front();
		//fprintf(stderr, "%d job: %d\n", pthread_self(), (int)*data);
		items.pop();
	}
	if(pthread_mutex_unlock(&mutex) != 0){
		//fprintf(stderr, "error!\n");
		return -1;
	}
		//fprintf(stderr, "%d wait end 2, job: %d\n", pthread_self(), (int)*data);
	return 1;
}


template <class T>
SelectableQueue<T>::SelectableQueue(){
    // 创建文件描述符
    // 建立管道，0读取，1写入
	if(pipe(fds) == -1){
		exit(0);
	}
	// 初始化锁
	pthread_mutex_init(&mutex, NULL);
}

template <class T>
SelectableQueue<T>::~SelectableQueue(){
    // 删除锁，关闭文件描述符
	pthread_mutex_destroy(&mutex);
	close(fds[0]);
	close(fds[1]);
}

template <class T>
int SelectableQueue<T>::push(const T item){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	{
		items.push(item);
	}
	// 向文件描述符写数据
	if(::write(fds[1], "1", 1) == -1){
		exit(0);
	}
	pthread_mutex_unlock(&mutex);
	return 1;
}

/**
 * 从队列中读取数据，并放到data中
 */
template <class T>
int SelectableQueue<T>::pop(T *data){
	int n, ret = 1;
	char buf[1];

	while(1){
	    // 读文件描述符，如果没有数据，将等待
		n = ::read(fds[0], buf, 1);
		if(n < 0){
			if(errno == EINTR){
				continue;
			}else{
				return -1;
			}
		}else if(n == 0){
			ret = -1;
		}else{
		    // 加锁
			if(pthread_mutex_lock(&mutex) != 0){
				return -1;
			}
			{
				if(items.empty()){
					fprintf(stderr, "%s %d error!\n", __FILE__, __LINE__);
					pthread_mutex_unlock(&mutex);
					return -1;
				}
				*data = items.front();
				items.pop();
			}
			pthread_mutex_unlock(&mutex);
		}
		break;
	}
	return ret;
}


// 初始化worker pool，只指定了一个名字
// 注意模板类的用法，有两个模板类的时候怎么写
template<class W, class JOB>
WorkerPool<W, JOB>::WorkerPool(const char *name){
	this->name = name;
	this->started = false;
}

// 析构函数，如果已经开始运行，则停止运行。除此之外每别的需要做的
template<class W, class JOB>
WorkerPool<W, JOB>::~WorkerPool(){
	if(started){
		stop();
	}
}

// 添加一个任务
template<class W, class JOB>
int WorkerPool<W, JOB>::push(JOB job){
	return this->jobs.push(job);
}

// 获取任务执行的结果
template<class W, class JOB>
int WorkerPool<W, JOB>::pop(JOB *job){
	return this->results.pop(job);
}

// 根据指定的参数，开始运行
// 这个应该在线程中调用？
template<class W, class JOB>
void* WorkerPool<W, JOB>::_run_worker(void *arg){
    // 获取到运行参数
	struct run_arg *p = (struct run_arg*)arg;
	int id = p->id;
	// 获取到运行的worker pool
	WorkerPool *tp = p->tp;
	// 为啥删除掉原来的运行参数？
	delete p;

    // 初始化一个worker？
	W w(tp->name);
	// 转化为worker对象。想必W类一定是worker的子类
	Worker *worker = (Worker *)&w;
	// 设置id
	worker->id = id;
	// 初始化
	worker->init();
	while(1){
		JOB job;
		// 拿出一个任务
		if(tp->jobs.pop(&job) == -1){
			fprintf(stderr, "jobs.pop error\n");
			::exit(0);
			break;
		}
		// 处理任务
		worker->proc(&job);
		// 把任务结果放回去
		if(tp->results.push(job) == -1){
			fprintf(stderr, "results.push error\n");
			::exit(0);
			break;
		}
	}
	// 运行完了，退出
	worker->destroy();
	// 这是什么东西？返回这个有什么用？
	return (void *)NULL;
}

// 开始运行工作池
template<class W, class JOB>
int WorkerPool<W, JOB>::start(int num_workers){
    // worker的数量
	this->num_workers = num_workers;
	// 如果已经开始，就没必要再重新开始了
	if(started){
		return 0;
	}
	int err;
	pthread_t tid;
	// 循环构建
	for(int i=0; i<num_workers; i++){
	    // 初始化运行参数
		struct run_arg *arg = new run_arg();
		arg->id = i;
		arg->tp = this;

        // 起新线程，开始运行
		err = pthread_create(&tid, NULL, &WorkerPool::_run_worker, arg);
		if(err != 0){
			fprintf(stderr, "can't create thread: %s\n", strerror(err));
		}else{
		    // 记录线程ID
			tids.push_back(tid);
		}
	}
	started = true;
	return 0;
}

template<class W, class JOB>
int WorkerPool<W, JOB>::stop(){
	// TODO: notify works quit and wait
	for(int i=0; i<tids.size(); i++){
#ifdef OS_ANDROID
#else
        // 停止线程
		pthread_cancel(tids[i]);
#endif
	}
	return 0;
}



#if 0
class MyWorker : public WorkerPool<MyWorker, int>::Worker{
	public:
		int proc(int *job){
			*job = (id + 1) * 100000 + *job;
			return 0;
		}
};

int main(){
	int num_jobs = 1000;
	WorkerPool<MyWorker, int> tp(10);
	tp.start();
	for(int i=0; i<num_jobs; i++){
		//usleep(200 * 1000);
		//printf("job: %d\n", i);
		tp.push_job(i);
	}
	printf("add end\n");
	for(int i=0; i<num_jobs; i++){
		int job;
		tp.pop_result(&job);
		printf("result: %d, %d\n", i, job);
	}
	printf("end\n");
	//tp.stop();
	return 0;
}
#endif

#endif


