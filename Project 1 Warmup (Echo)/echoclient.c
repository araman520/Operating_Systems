#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

/* Be prepared accept a response of this length */
#define BUFSIZE 256

#define USAGE                                                                       \
    "usage:\n"                                                                      \
    "  echoclient [options]\n"                                                      \
    "options:\n"                                                                    \
    "  -s                  Server (Default: localhost)\n"                           \
    "  -p                  Port (Default: 6200)\n"                                  \
    "  -m                  Message to send to server (Default: \"hello world.\")\n" \
    "  -h                  Show this help message\n"


/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"server", required_argument, NULL, 's'},
    {"port", required_argument, NULL, 'p'},
    {"message", required_argument, NULL, 'm'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}};

/* Main ========================================================= */
int main(int argc, char **argv)
{
    int option_char = 0;
    char *hostname = "localhost";
    unsigned short portno = 6200;
    char *message = "Hello world!!";

    // Parse and set command line arguments
    while ((option_char = getopt_long(argc, argv, "s:p:m:hx", gLongOptions, NULL)) != -1)
    {
        switch (option_char)
        {
        case 's': // server
            hostname = optarg;
            break;
        case 'p': // listen-port
            portno = atoi(optarg);
            break;
        default:
            fprintf(stderr, "%s", USAGE);
            exit(1);
        case 'm': // message
            message = optarg;
            break;
        case 'h': // help
            fprintf(stdout, "%s", USAGE);
            exit(0);
            break;
        }
    }

    setbuf(stdout, NULL); // disable buffering

    if ((portno < 1025) || (portno > 65535))
    {
        fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__, portno);
        exit(1);
    }

    if (NULL == message)
    {
        fprintf(stderr, "%s @ %d: invalid message\n", __FILE__, __LINE__);
        exit(1);
    }

    if (NULL == hostname)
    {
        fprintf(stderr, "%s @ %d: invalid host name\n", __FILE__, __LINE__);
        exit(1);
    }

    /* Socket Code Here */

    if (strcmp(hostname,"localhost")==0) hostname = "127.0.0.1";

     
    char port_string[10];
    sprintf(port_string, "%d", portno);

    struct addrinfo hints, *results, *record;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, port_string, &hints, &results) != 0) 
    {
        perror("Failed to translate client socket.");
        exit(EXIT_FAILURE);
    }

    int client_socket;

    // loop through all the results and bind to the first we can
    for(record = results; record != NULL; record = record->ai_next)
    {
        client_socket = socket(record->ai_family, record->ai_socktype, record->ai_protocol);

        if (client_socket == -1) continue;
        
        if(connect(client_socket, record->ai_addr, record->ai_addrlen) != -1) break;

        close(client_socket);
    }

    if (record == NULL)  
    {
        perror("Failed to create or connect client socket.");
        exit(EXIT_FAILURE);
    }

    freeaddrinfo(results);

    send(client_socket, message, BUFSIZE, 0);

    char server_response[BUFSIZE];

    recv(client_socket, &server_response, sizeof(server_response), 0);

    printf("%s", server_response);

    close(client_socket);

    return 0;

}