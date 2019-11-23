#include <pthread.h>
#include <stdbool.h>
#include "threadpool.h"
#include <string>
#include <unistd.h>
#include <deque>

bool ThreadPool_work_queue_t::addTask(ThreadPool_work_t *task) {
    //Push the new task to the beginning of the queue
    queue.push_front(task);

    return true;
}

bool ThreadPool_add_work(ThreadPool_t *tp, thread_func_t func, void *arg) {
    bool result = false;

    ThreadPool_work_t *task = new ThreadPool_work_t;
    task->func = func;
    task->arg = arg;

    //Lock the queue, add the task, signal the conditional and unlock
    pthread_mutex_lock(&(tp->workQueue.queueLock));
    result = tp->workQueue.addTask(task);
    pthread_cond_signal(&(tp->workQueue.length_nonzero));
    pthread_mutex_unlock(&(tp->workQueue.queueLock));
    tp->workQueue.jobsToDelete.push_back(task);

    return result;
}

/**
* Get a task from the given ThreadPool object
* Parameters:
*     tp - The ThreadPool object being passed
* Return:
*     ThreadPool_work_t* - The next task to run
*/
ThreadPool_work_t *ThreadPool_get_work(ThreadPool_t *tp){
    //Get the last job, then pop it
    ThreadPool_work_t *task = tp->workQueue.queue.back();
    tp->workQueue.queue.pop_back();
    return task;
}

/**
* Run the next task from the task queue
* Parameters:
*     tp - The ThreadPool Object this thread belongs to
*/
void *Thread_run(ThreadPool_t *tp) {
    while (true){        
        //Acquire the lock on the work queue
        pthread_mutex_lock(&(tp->workQueue.queueLock));
        
        //Wait until there's a job in the queue. Waiting will automatically release the lock
        while ( tp->workQueue.queue.size() == 0 ){
            //Condition variable:
            pthread_cond_wait(&(tp->workQueue.length_nonzero), 
                        &(tp->workQueue.queueLock));
        }

        //Once waiting is over, re-acquire the lock, pop a task from the queue and decrement
        ThreadPool_work_t *task = ThreadPool_get_work(tp);
                
        //Release the lock on the work queue before running the function;
        pthread_mutex_unlock(&tp->workQueue.queueLock);

        //Call the function, then delete it since it was called with new
        task->func(task->arg);
    }
}

/**
* A C style constructor for creating a new ThreadPool object
* Parameters:
*     num - The number of threads to create
* Return:
*     ThreadPool_t* - The pointer to the newly created ThreadPool object
*/
ThreadPool_t *ThreadPool_create(int num) {
    int s, tnum;
    ThreadPool_t *tp = new ThreadPool_t;
    pthread_attr_t attr;

    //Initialize attributes
    s = pthread_attr_init(&attr);
    if (s != 0)
        perror("pthread_attr_init");

    for (tnum = 0; tnum < num; tnum++) {
        /* The pthread_create() call stores the thread ID into
            corresponding element of tp[] */
        tp->threads.push_back(0);

        s = pthread_create(&(tp->threads[tnum]), &attr,
                            (void *(*)(void *))Thread_run, tp);
        if (s != 0)
            perror("pthread_create");
    }

    //Destroy the attribute once creating the threadpool is done
    s = pthread_attr_destroy(&attr);
    if (s != 0)
        perror("pthread_attr_destroy");

    return tp;
}

/**
* A C style destructor to destroy a ThreadPool object
* Parameters:
*     tp - The pointer to the ThreadPool object to be destroyed
*/
void ThreadPool_destroy(ThreadPool_t *tp) {

    //Add n pthread_exit() functions, which will terminate all the threads
    //This will make the threadpool wait for all the threads to finish before
    //Exiting
    for (unsigned int i=0; i<tp->threads.size(); i++){
        ThreadPool_add_work(tp, (thread_func_t) pthread_exit, 0);
    }

    //Pthread_join on all threads to get results
    for (unsigned int i=0; i<tp->threads.size(); i++){
        pthread_join(tp->threads[i], NULL);
    }

    for (unsigned int i=0; i<tp->workQueue.jobsToDelete.size(); i++){
        delete tp->workQueue.jobsToDelete[i];
    }
    //Delete the threadPool_T object since it was initialized using new
    delete tp;
}