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
#include <sys/mman.h>
#include <unistd.h>

#include <sys/ipc.h> 
#include <sys/msg.h> 

#include "gfserver.h"
#include "cache-student.h"



/* note that the -n and -z parameters are NOT used for Part 1 */
/* they are only used for Part 2 */                         
#define USAGE                                                                         \
"usage:\n"                                                                            \
"  webproxy [options]\n"                                                              \
"options:\n"                                                                          \
"  -n [segment_count]  Number of segments to use (Default: 4)\n"                      \
"  -p [listen_port]    Listen port (Default: 20121)\n"                                 \
"  -t [thread_count]   Num worker threads (Default: 9, Range: 1-520)\n"              \
"  -s [server]         The server to connect to (Default: Udacity S3 instance)\n"     \
"  -z [segment_size]   The segment size (in bytes, Default: 5014).\n"                  \
"  -h                  Show this help message\n"


/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
  {"segment-count", required_argument,      NULL,           'n'},
  {"port",          required_argument,      NULL,           'p'},
  {"thread-count",  required_argument,      NULL,           't'},
  {"server",        required_argument,      NULL,           's'},
  {"segment-size",  required_argument,      NULL,           'z'},         
  {"help",          no_argument,            NULL,           'h'},
  {"hidden",        no_argument,            NULL,           'i'}, /* server side */
  {NULL,            0,                      NULL,            0}
};

steque_t *seg_q; //Command channel queue
steque_t *buff_q; //Data channel queue

//Cache dedicated message queue
struct mesg_buffer cache_message_tx, cache_message_rx;
int cache_msgid_tx, cache_msgid_rx;

size_t SEG_SIZE; //Data segment size

extern ssize_t handle_with_cache(gfcontext_t *ctx, char *path, void* arg);

static gfserver_t gfs;

static void _sig_handler(int signo){
  if (signo == SIGTERM || signo == SIGINT){

    int shmid;
    struct shmseg* seg_ptr;

    while(steque_size(seg_q) != 0)
    {
      seg_ptr = steque_pop(seg_q);

      //Destroy command channels
      shmid = shmget(seg_ptr->skey, sizeof(struct shmseg), 0644|IPC_CREAT);
      shmctl(shmid, IPC_RMID, 0);

      //Destroy data channels
      shmid = shmget(seg_ptr->mkey, sizeof(struct shmseg), 0644|IPC_CREAT);
      shmctl(shmid, IPC_RMID, 0);

      //Destroy sync message queues 
      shmid = msgget(seg_ptr->skey + 173, 0666 | IPC_CREAT); 
      msgctl(shmid, IPC_RMID, NULL);
      shmid = msgget(seg_ptr->skey + 255, 0666 | IPC_CREAT); 
      msgctl(shmid, IPC_RMID, NULL);

    }

    //Destroy command channel queue
    steque_destroy(seg_q);
  	free(seg_q);

    //Destroy data channel queue
    steque_destroy(buff_q);
  	free(buff_q);

    gfserver_stop(&gfs);
    exit(signo);
  }
}

/* Main ========================================================= */
int main(int argc, char **argv) {
  int i;
  int option_char = 0;
  unsigned short port = 20121;
  unsigned short nworkerthreads = 9;
  unsigned int nsegments = 4;
  size_t segsize = 5014;
  char *server = "https://raw.githubusercontent.com/gt-cs6200/image_data";

  /* disable buffering on stdout so it prints immediately */
  setbuf(stdout, NULL);

  if (signal(SIGINT, _sig_handler) == SIG_ERR) {
    fprintf(stderr,"Can't catch SIGINT...exiting.\n");
    exit(SERVER_FAILURE);
  }

  if (signal(SIGTERM, _sig_handler) == SIG_ERR) {
    fprintf(stderr,"Can't catch SIGTERM...exiting.\n");
    exit(SERVER_FAILURE);
  }

  /* Parse and set command line arguments */
  while ((option_char = getopt_long(argc, argv, "s:qt:hn:xp:z:l", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      default:
        fprintf(stderr, "%s", USAGE);
        exit(__LINE__);
      case 'p': // listen-port
        port = atoi(optarg);
        break;
      case 'n': // segment count
        nsegments = atoi(optarg);
        break;   
      case 's': // file-path
        server = optarg;
        break;                                          
      case 'z': // segment size
        segsize = atoi(optarg);
        break;
      case 't': // thread-count
        nworkerthreads = atoi(optarg);
        break;
      case 'i':
      case 'x':
      case 'l':
        break;
      case 'h': // help
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;
    }
  }

  if (segsize < 128) {
    fprintf(stderr, "Invalid segment size\n");
    exit(__LINE__);
  }

  if (!server) {
    fprintf(stderr, "Invalid (null) server name\n");
    exit(__LINE__);
  }

  if (port < 1024) {
    fprintf(stderr, "Invalid port number\n");
    exit(__LINE__);
  }

  if (nsegments < 1) {
    fprintf(stderr, "Must have a positive number of segments\n");
    exit(__LINE__);
  }

  if ((nworkerthreads < 1) || (nworkerthreads > 520)) {
    fprintf(stderr, "Invalid number of worker threads\n");
    exit(__LINE__);
  }

  //Cache dedicated request message queue
	cache_msgid_tx = msgget(100, 0666 | IPC_CREAT); 
	cache_msgid_rx = msgget(101, 0666 | IPC_CREAT); 
	cache_message_tx.mesg_type = 1;
  cache_message_rx.mesg_type = 1;

  SEG_SIZE = segsize;

  //Command channel queue
  seg_q = malloc(sizeof(steque_t));
  steque_init(seg_q);

  //Data channel queue
  buff_q = malloc(sizeof(steque_t));
  steque_init(buff_q);

  for(int n1=0; n1<nsegments; n1++)
  {
    int shmid;
    struct shmseg *shmp;

    int buffer_id;
    void* buffer;

    key_t skey;
    key_t mkey;

    skey = ftok ("./", n1+1);
    mkey = ftok ("./", 200+n1+1);

    //Command channel
    shmid = shmget(skey, sizeof(struct shmseg), 0644|IPC_CREAT);
    shmp = shmat(shmid, NULL, 0);
    if(shmp == (void *) -1) printf("Segment error\n");

    //Data channel
    buffer_id = shmget(mkey, segsize, 0666|IPC_CREAT);
    buffer = shmat(buffer_id, NULL, 0);
    if(buffer == (void *) -1) printf("Buffer error\n");

    //Initialize command channel variables
    shmp->skey = skey;
    shmp->mkey = mkey;
    shmp->mesg_control = 0;
    shmp->mesg_size = 0;
    shmp->file_size = 0;
    shmp->mesg_seg_size = segsize;

    //Sync message queues for command channel
    key_t synt_key = shmp->skey + 173;
	  key_t synr_key = shmp->skey + 255;
    shmp->syn_msgid_tx = msgget(synt_key, 0666 | IPC_CREAT); 
	  shmp->syn_msgid_rx = msgget(synr_key, 0666 | IPC_CREAT);
    shmp->synt.mesg_type = 1;
    shmp->synr.mesg_type = 1;

    //Enqueue command and data channel
    steque_enqueue(seg_q, (void*)shmp);
    steque_enqueue(buff_q, (void*)buffer);

    printf("UID - %d\n", shmp->skey);
    printf("Segment %d enqueued\n", n1);

  }


  // Initialize server structure here
  gfserver_init(&gfs, nworkerthreads);

  // Set server options here
  gfserver_setopt(&gfs, GFS_MAXNPENDING, 321);
  gfserver_setopt(&gfs, GFS_WORKER_FUNC, handle_with_cache);
  gfserver_setopt(&gfs, GFS_PORT, port);

  // Set up arguments for worker here
  for(i = 0; i < nworkerthreads; i++) {
    gfserver_setopt(&gfs, GFS_WORKER_ARG, i, server);
  }
  
  // Invoke the framework - this is an infinite loop and shouldn't return
  gfserver_serve(&gfs);

  // not reached
  return -1;

}