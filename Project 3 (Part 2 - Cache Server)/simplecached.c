#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <sys/signal.h>
#include <printf.h>
#include <curl/curl.h>

#include <sys/ipc.h> 
#include <sys/msg.h>

#include <fcntl.h> 
#include <sys/shm.h> 
#include <sys/stat.h>
#include <sys/mman.h>

#include "gfserver.h"
#include "cache-student.h"
#include "shm_channel.h"
#include "simplecache.h"

#include "steque.h"
#include <pthread.h>

steque_t *p_q;

//Sync variables for boss-worker queue
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER; //mutex
pthread_cond_t c_worker = PTHREAD_COND_INITIALIZER; //Conditional Variable

//Sync variable for data channel mapping
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;


#if !defined(CACHE_FAILURE)
#define CACHE_FAILURE (-1)
#endif // CACHE_FAILURE

#define MAX_CACHE_REQUEST_LEN 5041

static void _sig_handler(int signo){
	if (signo == SIGTERM || signo == SIGINT){
		// you should do IPC cleanup here

		key_t cache_key_rx = 100;
		key_t cache_key_tx = 101;

		int cache_msgid_rx = msgget(cache_key_rx, 0666 | IPC_CREAT); 
		int cache_msgid_tx = msgget(cache_key_tx, 0666 | IPC_CREAT);

		//Delete Cache dedicated request message queue
		msgctl(cache_msgid_rx, IPC_RMID, NULL);
		msgctl(cache_msgid_tx, IPC_RMID, NULL);

		//Destroy request queue
		steque_destroy(p_q);
  		free(p_q);

		exit(signo);
	}
}

unsigned long int cache_delay;

#define USAGE                                                                 \
"usage:\n"                                                                    \
"  simplecached [options]\n"                                                  \
"options:\n"                                                                  \
"  -c [cachedir]       Path to static files (Default: ./)\n"                  \
"  -t [thread_count]   Thread count for work queue (Default is 7, Range is 1-31415)\n"      \
"  -d [delay]          Delay in simplecache_get (Default is 0, Range is 0-5000000 (microseconds)\n "	\
"  -h                  Show this help message\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
  {"cachedir",           required_argument,      NULL,           'c'},
  {"nthreads",           required_argument,      NULL,           't'},
  {"help",               no_argument,            NULL,           'h'},
  {"hidden",			 no_argument,			 NULL,			 'i'}, /* server side */
  {"delay", 			 required_argument,		 NULL, 			 'd'}, // delay.
  {NULL,                 0,                      NULL,             0}
};

void Usage() {
  fprintf(stdout, "%s", USAGE);
}


void *worker_handler (void *param) //Worker handler
{
	int file_index;
	size_t bytes_sent;
	size_t read_bytes;
	struct shmseg* seg_ptr;//Proxy Command Channel
	void* shared_mem_buffer;//Proxy data channel
	int shared_mem_buffer_id;//Proxy data channel id
	struct stat buf;

	while(1)
	{
		pthread_mutex_lock(&m);
			while(steque_size(p_q) == 0) pthread_cond_wait(&c_worker, &m);
			seg_ptr = steque_pop(p_q); //Control Channel shared memory pointer
		pthread_mutex_unlock(&m);
		pthread_cond_signal(&c_worker);

		//Receive requested path from Proxy
		msgrcv(seg_ptr->syn_msgid_tx, &seg_ptr->synt, sizeof(seg_ptr->synt.mesg_text), 1, 0);

		//Check if file exists in Cache
		file_index = simplecache_get(seg_ptr->mesg_path);

		if(file_index == -1)//File not in cache
		{
			//Telling Proxy file is not in Cache
			seg_ptr->mesg_control = -1;
			msgsnd(seg_ptr->syn_msgid_rx, &seg_ptr->synr, sizeof(seg_ptr->synr.mesg_text), 0);
		}
		else
		{
			//Map shared memory segment
			shared_mem_buffer_id = shmget(seg_ptr->mkey, seg_ptr->mesg_seg_size, 0666|IPC_CREAT);
			pthread_mutex_lock(&m2);
				shared_mem_buffer = shmat(shared_mem_buffer_id, NULL, 0);
			pthread_mutex_unlock(&m2);
			if(shared_mem_buffer == (void *) -1) printf("Buffer error\n");

			//Get requested file size
			fstat(file_index, &buf);
			seg_ptr->file_size = buf.st_size;

			//Send file size to Proxy
			seg_ptr->mesg_control = 1;
			msgsnd(seg_ptr->syn_msgid_rx, &seg_ptr->synr, sizeof(seg_ptr->synr.mesg_text), 0);
			msgrcv(seg_ptr->syn_msgid_tx, &seg_ptr->synt, sizeof(seg_ptr->synt.mesg_text), 1, 0);

			//Send file in chuncks proportional to how large shared memory segment is

			//Read from file in to the shared memory
			bytes_sent = 0;
			read_bytes = pread(file_index, shared_mem_buffer, seg_ptr->mesg_seg_size, bytes_sent);

			while(read_bytes>0)
			{
				bytes_sent += read_bytes;

				//Let Proxy know data is in shared memory
				seg_ptr->mesg_size = read_bytes;
				msgsnd(seg_ptr->syn_msgid_rx, &seg_ptr->synr, sizeof(seg_ptr->synr.mesg_text), 0);
				msgrcv(seg_ptr->syn_msgid_tx, &seg_ptr->synt, sizeof(seg_ptr->synt.mesg_text), 1, 0);

				//Once Proxy has read data, read next chunk from file in to shared memory
				read_bytes = pread(file_index, shared_mem_buffer, seg_ptr->mesg_seg_size, bytes_sent);

			}

			//Let Proxy know that entire file has been sent
			seg_ptr->mesg_control = 2;
			msgsnd(seg_ptr->syn_msgid_rx, &seg_ptr->synr, sizeof(seg_ptr->synr.mesg_text), 0);

			//Unmap shared memory segment
			shmdt(shared_mem_buffer);

		}
			
	}

}

int main(int argc, char **argv) {
	int nthreads = 7;
	char *cachedir = "locals.txt";
	char option_char;

	/* disable buffering to stdout */
	setbuf(stdout, NULL);

	while ((option_char = getopt_long(argc, argv, "id:c:hlxt:", gLongOptions, NULL)) != -1) {
		switch (option_char) {
			default:
				Usage();
				exit(1);
			case 'c': //cache directory
				cachedir = optarg;
				break;
			case 'h': // help
				Usage();
				exit(0);
				break;    
			case 't': // thread-count
				nthreads = atoi(optarg);
				break;
			case 'd':
				cache_delay = (unsigned long int) atoi(optarg);
				break;
			case 'i': // server side usage
			case 'l': // experimental
			case 'x': // experimental
				break;
		}
	}

	if (cache_delay > 5000000) {
		fprintf(stderr, "Cache delay must be less than 5000000 (us)\n");
		exit(__LINE__);
	}

	if ((nthreads>31415) || (nthreads < 1)) {
		fprintf(stderr, "Invalid number of threads\n");
		exit(__LINE__);
	}

	if (SIG_ERR == signal(SIGINT, _sig_handler)){
		fprintf(stderr,"Unable to catch SIGINT...exiting.\n");
		exit(CACHE_FAILURE);
	}

	if (SIG_ERR == signal(SIGTERM, _sig_handler)){
		fprintf(stderr,"Unable to catch SIGTERM...exiting.\n");
		exit(CACHE_FAILURE);
	}

	// Initialize cache
	simplecache_init(cachedir);

	// cache code gos here

	p_q = malloc(sizeof(steque_t)); //Create request queue

	steque_init(p_q); //Intitalize request queue

	pthread_t workers[nthreads]; //Initalize workers

	for(int i=0; i<nthreads; i++)
	{
		if(pthread_create(&workers[i], NULL, worker_handler, NULL) == 0) //Create workers
		{
		printf("Worker %d created\n", i+1);
		}
		else
		{
		printf("Could not create worker %d\n", i);
		exit(1);
		}
	}

	//Create Cache dedicated request message queue

	struct mesg_buffer cache_message_tx, cache_message_rx;

	key_t cache_key_rx = 100;
	key_t cache_key_tx = 101;

	int cache_msgid_rx = msgget(cache_key_rx, 0666 | IPC_CREAT); 
	int cache_msgid_tx = msgget(cache_key_tx, 0666 | IPC_CREAT); 

	cache_message_rx.mesg_type = 1;
    cache_message_tx.mesg_type = 1;

	cache_message_tx.mesg_text = 100;

	while(1)
	{
		//Receive request(Proxy command channel ID) on cache dedicated message queue
		msgrcv(cache_msgid_rx, &cache_message_rx, sizeof(cache_message_rx.mesg_text), 1, 0);

		//Map Proxy command channel
		struct shmseg *shmp;
		int shmid;
		shmid = shmget(cache_message_rx.mesg_text, sizeof(struct shmseg), 0644|IPC_CREAT);
		pthread_mutex_lock(&m2);
    		shmp = shmat(shmid, NULL, 0);
		pthread_mutex_unlock(&m2);

		if(shmp == (void *) -1) printf("Segment error\n");

		//Enqueue Proxy command channel, so worker can work on request
		pthread_mutex_lock(&m);
      		steque_enqueue(p_q, (void*)shmp);
		pthread_mutex_unlock(&m);
		pthread_cond_signal(&c_worker);

		//Send ACK to Proxy
		msgsnd(cache_msgid_tx, &cache_message_tx, sizeof(cache_message_tx.mesg_text), 0);

	}
	// Won't execute
	return 0;
}