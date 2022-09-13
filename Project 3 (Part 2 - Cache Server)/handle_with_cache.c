#include "gfserver.h"
#include "cache-student.h"
#include <stdio.h> 
#include <sys/ipc.h> 
#include <sys/msg.h> 
#include <sys/mman.h>
#include <unistd.h>

#include "steque.h"
#include <pthread.h>

#define BUFSIZE (6201)
int P = 0;

extern steque_t *seg_q;
extern steque_t *buff_q;

extern size_t SEG_SIZE;

extern struct mesg_buffer cache_message_tx, cache_message_rx;
extern int cache_msgid_tx, cache_msgid_rx;

//Sync Variable to access cache dedicated message queue
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;

//Sync variables for command and data channel queue
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t c_worker2 = PTHREAD_COND_INITIALIZER;

ssize_t handle_with_cache(gfcontext_t *ctx, const char *path, void* arg)
{
	struct shmseg* seg_ptr; //Command channel pointer
	void* shared_memory_buffer; //Data channel pointer

	//Get an available command and data channel pair
	pthread_mutex_lock(&m2);
		while(steque_size(seg_q) == 0) pthread_cond_wait(&c_worker2, &m2);
		seg_ptr = steque_pop(seg_q); //Command channel shared memory
		shared_memory_buffer = steque_pop(buff_q); //Data channel shared memory
	pthread_mutex_unlock(&m2);
	pthread_cond_signal(&c_worker2);

	//Send request to Cache on Cache's dedicated message queue
	//The message sent is the command channel shared memory ID
	pthread_mutex_lock(&m1);
		cache_message_tx.mesg_text = seg_ptr->skey;
		msgsnd(cache_msgid_tx, &cache_message_tx, sizeof(cache_message_tx.mesg_text), 0);
		msgrcv(cache_msgid_rx, &cache_message_rx, sizeof(cache_message_rx.mesg_text), 1, 0);
	pthread_mutex_unlock(&m1);

	//Send request path to Proxy on command channel
	sprintf(seg_ptr->mesg_path, "%s", path);
	msgsnd(seg_ptr->syn_msgid_tx, &seg_ptr->synt, sizeof(seg_ptr->synt.mesg_text), 0);
	msgrcv(seg_ptr->syn_msgid_rx, &seg_ptr->synr, sizeof(seg_ptr->synr.mesg_text), 1, 0);

	if(seg_ptr->mesg_control == -1)//File not found
	{
		pthread_mutex_lock(&m2);
			steque_enqueue(seg_q, (void*)seg_ptr);
			steque_enqueue(buff_q, (void*)shared_memory_buffer);
		pthread_mutex_unlock(&m2);
		pthread_cond_signal(&c_worker2);

		gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);//Send FNF header to client

		return 0;

	}
	else
	{
		gfs_sendheader(ctx, GF_OK, seg_ptr->file_size);//Send header to client

		//Let Cache know Proxy is ready to receive the file
		msgsnd(seg_ptr->syn_msgid_tx, &seg_ptr->synt, sizeof(seg_ptr->synt.mesg_text), 0);
		msgrcv(seg_ptr->syn_msgid_rx, &seg_ptr->synr, sizeof(seg_ptr->synr.mesg_text), 1, 0);

		size_t dc = 0;
		size_t bytes_transferred = 0;
		ssize_t write_len;

		while(1)
		{
			if(seg_ptr->mesg_control == 2) break;//Entire file received

			//Read from shared memory segment (data channel) and send it to client
			write_len = gfs_send(ctx, shared_memory_buffer, seg_ptr->mesg_size);

			bytes_transferred += write_len;
			dc += seg_ptr->mesg_size;

			//Let Cache know Proxy is ready to receive more data
			msgsnd(seg_ptr->syn_msgid_tx, &seg_ptr->synt, sizeof(seg_ptr->synt.mesg_text), 0);
			msgrcv(seg_ptr->syn_msgid_rx, &seg_ptr->synr, sizeof(seg_ptr->synr.mesg_text), 1, 0);

		}

		//Requeue commonad and data channels
		pthread_mutex_lock(&m2);
			steque_enqueue(seg_q, (void*)seg_ptr);
			steque_enqueue(buff_q, (void*)shared_memory_buffer);
		pthread_mutex_unlock(&m2);
		pthread_cond_signal(&c_worker2);

		return bytes_transferred;

	}	

}