#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <getopt.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>

#define BUFSIZE 256

#define USAGE                                                                 \
"usage:\n"                                                                    \
"  echoserver [options]\n"                                                    \
"options:\n"                                                                  \
"  -p                  Port (Default: 6200)\n"                                \
"  -m                  Maximum pending connections (default: 1)\n"            \
"  -h                  Show this help message\n"                              \


/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
  {"port",          required_argument,      NULL,           'p'},
  {"maxnpending",   required_argument,      NULL,           'm'},
  {"help",          no_argument,            NULL,           'h'},
  {NULL,            0,                      NULL,             0}
};


int main(int argc, char **argv) {
  int option_char;
  int portno = 6200; /* port to listen on */
  int maxnpending = 1;
  
  // Parse and set command line arguments
  while ((option_char = getopt_long(argc, argv, "p:m:hx", gLongOptions, NULL)) != -1) {
   switch (option_char) {
      case 'p': // listen-port
        portno = atoi(optarg);
        break;                                        
      default:
        fprintf(stderr, "%s ", USAGE);
        exit(1);
      case 'm': // server
        maxnpending = atoi(optarg);
        break; 
      case 'h': // help
        fprintf(stdout, "%s ", USAGE);
        exit(0);
        break;
    }
  }

    setbuf(stdout, NULL); // disable buffering

    if ((portno < 1025) || (portno > 65535)) {
        fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__, portno);
        exit(1);
    }
    if (maxnpending < 1) {
        fprintf(stderr, "%s @ %d: invalid pending count (%d)\n", __FILE__, __LINE__, maxnpending);
        exit(1);
    }


  /* Socket Code Here */

    struct addrinfo hints, *results, *record;

    char port_string[10];
    sprintf(port_string, "%d", portno);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char *hostname = "127.0.0.1";

    if (getaddrinfo(hostname, port_string, &hints, &results) != 0) 
    {
        perror("Failed to translate client socket.");
        exit(EXIT_FAILURE);
    }

    // loop through all the results and bind to the first we can

    int server_socket;
    int enable = 1;

    for(record = results; record != NULL; record = record->ai_next) {
        server_socket = socket(record->ai_family, record->ai_socktype, record->ai_protocol);

        if(server_socket == -1) continue;

        setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

        if (bind(server_socket, record->ai_addr, record->ai_addrlen) != -1) break;

        close(server_socket);
    }

    freeaddrinfo(results);

    while(1)
    {
      listen(server_socket, maxnpending);

      int client_socket;

      client_socket = accept(server_socket, NULL, NULL);

      char client_response[BUFSIZE];

      recv(client_socket, &client_response, sizeof(client_response), 0);

      printf("%s", client_response);

      send(client_socket, client_response, BUFSIZE, 0);
    }

    //close(server_socket);

    return 0;
}