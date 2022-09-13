
#include "gfserver-student.h"
#include "steque.h"
#include <pthread.h>


#define USAGE                                                               \
  "usage:\n"                                                                \
  "  gfserver_main [options]\n"                                             \
  "options:\n"                                                              \
  "  -t [nthreads]       Number of threads (Default: 7)\n"                  \
  "  -p [listen_port]    Listen port (Default: 20121)\n"                    \
  "  -m [content_file]   Content file mapping keys to content files\n"      \
  "  -d [delay]          Delay in content_get, default 0, range 0-5000000 " \
  "(microseconds)\n "                                                       \
  "  -h                  Show this help message.\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"content", required_argument, NULL, 'm'},
    {"delay", required_argument, NULL, 'd'},
    {"nthreads", required_argument, NULL, 't'},
    {"port", required_argument, NULL, 'p'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}};

extern unsigned long int content_delay;

extern gfh_error_t gfs_handler(gfcontext_t **ctx, const char *path, void *arg); //Server handler

steque_t *c_q; //queue for client context
steque_t *p_q; //queue for requested path
steque_t *a_q; //queue for argument

extern void *worker_handler (void *param); //Worker thread handler

static void _sig_handler(int signo) {
  if ((SIGINT == signo) || (SIGTERM == signo)) {
    exit(signo);
  }
}


/* Main ========================================================= */
int main(int argc, char **argv) {
  int option_char = 0;
  unsigned short port = 20121;
  char *content_map = "content.txt";
  gfserver_t *gfs = NULL;
  int nthreads = 7;

  setbuf(stdout, NULL);

  if (SIG_ERR == signal(SIGINT, _sig_handler)) {
    fprintf(stderr, "Can't catch SIGINT...exiting.\n");
    exit(EXIT_FAILURE);
  }

  if (SIG_ERR == signal(SIGTERM, _sig_handler)) {
    fprintf(stderr, "Can't catch SIGTERM...exiting.\n");
    exit(EXIT_FAILURE);
  }

  // Parse and set command line arguments
  while ((option_char = getopt_long(argc, argv, "p:t:rhm:d:", gLongOptions,
                                    NULL)) != -1) {
    switch (option_char) {
      case 't':  // nthreads
        nthreads = atoi(optarg);
        break;
      case 'h':  // help
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;
      case 'p':  // listen-port
        port = atoi(optarg);
        break;
      default:
        fprintf(stderr, "%s", USAGE);
        exit(1);
      case 'm':  // file-path
        content_map = optarg;
        break;
      case 'd':  // delay
        content_delay = (unsigned long int)atoi(optarg);
        break;
    }
  }

  /* not useful, but it ensures the initial code builds without warnings */
  if (nthreads < 1) {
    nthreads = 1;
  }

  if (content_delay > 5000000) {
    fprintf(stderr, "Content delay must be less than 5000000 (microseconds)\n");
    exit(__LINE__);
  }

  content_init(content_map);

  /* Initialize thread management */

  c_q = malloc(sizeof(steque_t)); //Allocate memory for client context queue
  p_q = malloc(sizeof(steque_t)); //Allocate memory for requested path queue
  a_q = malloc(sizeof(steque_t)); //Allocate memory for argument queue

  steque_init(c_q); //Initialize client context queue
  steque_init(p_q); //Initialize requested path queue
  steque_init(a_q); //Initialize argument queue

  pthread_t workers[nthreads]; //Woker thread array

  //Create worker threads

  for(int i=0; i<nthreads; i++)
  {
    if(pthread_create(&workers[i], NULL, worker_handler, NULL) == 0)
    {
      printf("Worker %d created\n", i+1);
    }
    else
    {
      printf("Could not create worker %d\n", i);
      exit(1);
    }
  }


  /*Initializing server*/
  gfs = gfserver_create();

  /*Setting options*/
  gfserver_set_port(&gfs, port);
  gfserver_set_maxpending(&gfs, 20);
  gfserver_set_handler(&gfs, gfs_handler);
  gfserver_set_handlerarg(&gfs, NULL);  // doesn't have to be NULL!

  /*Loops forever*/
  gfserver_serve(&gfs);
}
