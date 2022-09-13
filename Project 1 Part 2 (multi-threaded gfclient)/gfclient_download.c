#include "gfclient-student.h"
#include "steque.h"
#include <pthread.h>

#define MAX_THREADS (2012)

#define USAGE                                                   \
  "usage:\n"                                                    \
  "  webclient [options]\n"                                     \
  "options:\n"                                                  \
  "  -h                  Show this help message\n"              \
  "  -r [num_requests]   Request download total (Default: 2)\n" \
  "  -p [server_port]    Server port (Default: 20121)\n"        \
  "  -s [server_addr]    Server address (Default: 127.0.0.1)\n" \
  "  -t [nthreads]       Number of threads (Default 2)\n"       \
  "  -w [workload_path]  Path to workload file (Default: workload.txt)\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"help", no_argument, NULL, 'h'},
    {"nthreads", required_argument, NULL, 't'},
    {"nrequests", required_argument, NULL, 'r'},
    {"server", required_argument, NULL, 's'},
    {"port", required_argument, NULL, 'p'},
    {"workload-path", required_argument, NULL, 'w'},
    {NULL, 0, NULL, 0}};

static void Usage() { fprintf(stderr, "%s", USAGE); }

static void localPath(char *req_path, char *local_path) {
  static int counter = 0;

  sprintf(local_path, "%s-%06d", &req_path[1], counter++);
}

static FILE *openFile(char *path) {
  char *cur, *prev;
  FILE *ans;

  /* Make the directory if it isn't there */
  prev = path;
  while (NULL != (cur = strchr(prev + 1, '/'))) {
    *cur = '\0';

    if (0 > mkdir(&path[0], S_IRWXU)) {
      if (errno != EEXIST) {
        perror("Unable to create directory");
        exit(EXIT_FAILURE);
      }
    }

    *cur = '/';
    prev = cur;
  }

  if (NULL == (ans = fopen(&path[0], "w"))) {
    perror("Unable to open file");
    exit(EXIT_FAILURE);
  }

  return ans;
}

/* Callbacks ========================================================= */
static void writecb(void *data, size_t data_len, void *arg) {
  FILE *file = (FILE *)arg;

  fwrite(data, 1, data_len, file);

}

steque_t *p_q;

pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER; //mutex
pthread_cond_t c_worker = PTHREAD_COND_INITIALIZER; //Conditional Variable

int CAN_EXIT = 0; //Variable to check if all work is done

char *SERVER; //Global Server info
unsigned short PORT; //Global Port info

void *worker_handler (void *param) //Worker handler
{
	char *path;

	while(1)
	{
		pthread_mutex_lock(&m); //Lock mutex
	
			while(steque_size(p_q) == 0) //Check if queue empty
			{
        if(CAN_EXIT == 1) //Check if all work is complete
        {
          pthread_mutex_unlock(&m); //If all work complete, release mutex
          pthread_cond_signal(&c_worker); //Signal any worker waiting
          pthread_exit(NULL); //Worker thread exit
        }

				pthread_cond_wait(&c_worker, &m); //If queue empty and all work not done, wait on conditional variable

			}

			path = steque_pop(p_q); //if queue not empty, pop a request

		pthread_mutex_unlock(&m); //Unlock mutex

    pthread_cond_signal(&c_worker); //Signal any worker waiting

		char local_path[1066];

    localPath(path, local_path);

    FILE *file = openFile(local_path);

    gfcrequest_t *gfr;

    int returncode = 0;

    gfr = gfc_create();
    gfc_set_server(&gfr, SERVER);
    gfc_set_path(&gfr, path);
    gfc_set_port(&gfr, PORT);
    gfc_set_writefunc(&gfr, writecb);
    gfc_set_writearg(&gfr, file);

    if (0 > (returncode = gfc_perform(&gfr)))
    {
      fclose(file);
      unlink(local_path);
    } 
    else fclose(file);

    if (gfc_get_status(&gfr) != GF_OK) unlink(local_path);

    gfc_cleanup(&gfr);

	}

}

/* Main ========================================================= */
int main(int argc, char **argv) {
  /* COMMAND LINE OPTIONS ============================================= */
  char *server = "localhost";
  unsigned short port = 20121;
  char *workload_path = "workload.txt";
  int option_char = 0;
  int nrequests = 20;
  int nthreads = 4;
  int i = 0;

  setbuf(stdout, NULL);  // disable caching

  // Parse and set command line arguments
  while ((option_char = getopt_long(argc, argv, "p:n:hs:w:r:t:", gLongOptions,
                                    NULL)) != -1) {
    switch (option_char) {
      case 'h':  // help
        Usage();
        exit(0);
        break;
      case 'r':
        nrequests = atoi(optarg);
        break;
      case 'n':  // nrequests
        break;
      case 'p':  // port
        port = atoi(optarg);
        break;
      default:
        Usage();
        exit(1);
      case 's':  // server
        server = optarg;
        break;
      case 't':  // nthreads
        nthreads = atoi(optarg);
        break;
      case 'w':  // workload-path
        workload_path = optarg;
        break;
    }
  }

  if (EXIT_SUCCESS != workload_init(workload_path)) {
    fprintf(stderr, "Unable to load workload file %s.\n", workload_path);
    exit(EXIT_FAILURE);
  }

  if (nthreads < 1) {
    nthreads = 1;
  }
  if (nthreads > MAX_THREADS) {
    nthreads = MAX_THREADS;
  }

  gfc_global_init();

  SERVER = server; //Global Server info
  PORT = port; //Global Port info

  /* add your threadpool creation here */
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

  /* Build your queue of requests here */
  for (i = 0; i < nrequests; i++) 
  {
    /* Note that when you have a worker thread pool, you will need to move this
     * logic into the worker threads */
    char *req_path = workload_get_path();

    if (strlen(req_path) > 1024) {
      fprintf(stderr, "Request path exceeded maximum of 1024 characters\n.");
      exit(EXIT_FAILURE);
    }
  
		pthread_mutex_lock(&m); //Lock mutex
	
      steque_enqueue(p_q, (void*)req_path); //Enqueue request

      if(i == (nrequests-1)) CAN_EXIT = 1; //Check if all requests enqueued, if yes, threads can exit once all work is done

    pthread_mutex_unlock(&m); //Unlock mutex

    pthread_cond_signal(&c_worker); //Signal any waiting worker

  }

  for(int i=0; i<nthreads; i++) pthread_join(workers[i], NULL); //Wait for workers to exit

  printf("All threads completed execution.");

  steque_destroy(p_q); //Destroy queue
  free(p_q); //Free queue memory

  gfc_global_cleanup();  // use for any global cleanup for AFTER your thread
                         // pool has terminated.

  return 0;
}
