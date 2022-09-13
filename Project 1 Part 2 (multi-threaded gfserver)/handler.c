#include "gfserver-student.h"
#include "gfserver.h"
#include "content.h"

#include "workload.h"
#include <pthread.h>

#include "steque.h"

extern steque_t *c_q; //queue for client context
extern steque_t *p_q; //queue for requested path
extern steque_t *a_q; //queue for argument


pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER; //mutex
pthread_cond_t c_worker = PTHREAD_COND_INITIALIZER; //Conditional Variable


void gfs_actual_handler(gfcontext_t *ct2, const char *path, void* arg) //handler that actually handles the request after request is retrieved from queue
{

	int y = content_get(path); //Get actual path

	if(y == -1) gfs_sendheader(&ct2, GF_FILE_NOT_FOUND, 0); //Send file not found header if file does not exist
	else
	{
		struct stat buf;
		fstat(y, &buf);
		gfs_sendheader(&ct2, GF_OK, buf.st_size); //Send OK header if file exists
	}

	//Send the file

    char ch[2000];

    char *chp = ch;

    ssize_t bytes_sent = 0;

    int x = pread(y, chp, sizeof(ch), bytes_sent);

    ssize_t ds = 0;

    while(x>0)
	{

		ds = gfs_send(&ct2, chp, x);

		bytes_sent += ds;

		x = pread(y, chp, sizeof(chp), bytes_sent);

	}

    ct2 = NULL;

}

//
//  The purpose of this function is to handle a get request
//
//  The ctx is a pointer to the "context" operation and it contains connection state
//  The path is the path being retrieved
//  The arg allows the registration of context that is passed into this routine.
//  Note: you don't need to use arg. The test code uses it in some cases, but
//        not in others.
//

gfh_error_t gfs_handler(gfcontext_t **ctx, const char *path, void* arg)
{
	gfcontext_t *ct2 = *ctx; //New pointer that points to ctx

    pthread_mutex_lock(&m); //Lock mutex
	
		//Enqueue request details including new pointer to ctx
		steque_enqueue(c_q, (void*)ct2);
		steque_enqueue(p_q, (void*)path);
		steque_enqueue(a_q, (void*)arg);

	pthread_mutex_unlock(&m); //Unlock mutex

	pthread_cond_signal(&c_worker); //Signal any worker waiting

	*ctx = NULL; //Make original client context pointer null

	return gfh_success; //Return success regardless of actual outcome
	
}


void *worker_handler (void *param) //Worker thread handler
{
	gfcontext_t *ct2;
	char *path2;
	void* arg2;

	while(1)
	{
		pthread_mutex_lock(&m); //Lock mutex
	
			while(steque_size(c_q) == 0) //Check if queue empty
			{
				pthread_cond_wait(&c_worker, &m); //Wait on conditional variable if queue empty
			}

			//Pop request details from queue
			ct2 = steque_pop(c_q);
			path2 = steque_pop(p_q);
			arg2 = steque_pop(a_q);

		pthread_mutex_unlock(&m); //Unlock mutex

		pthread_cond_signal(&c_worker); //Signal any worker waiting

		gfs_actual_handler(ct2, path2, arg2); //Call actial handler to handle request

	}
}