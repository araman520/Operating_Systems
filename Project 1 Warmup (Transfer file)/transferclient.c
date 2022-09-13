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
#define BUFSIZE 820

#define USAGE                                                \
    "usage:\n"                                               \
    "  transferclient [options]\n"                           \
    "options:\n"                                             \
    "  -s                  Server (Default: localhost)\n"    \
    "  -p                  Port (Default: 20801)\n"           \
    "  -o                  Output file (Default cs6200.txt)\n" \
    "  -h                  Show this help message\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"server", required_argument, NULL, 's'},
    {"port", required_argument, NULL, 'p'},
    {"output", required_argument, NULL, 'o'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}};

/* Main ========================================================= */
int main(int argc, char **argv)
{
    int option_char = 0;
    char *hostname = "localhost";
    unsigned short portno = 20801;
    char *filename = "cs6200.txt";

    setbuf(stdout, NULL);

    // Parse and set command line arguments
    while ((option_char = getopt_long(argc, argv, "s:xp:o:h", gLongOptions, NULL)) != -1)
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
        case 'o': // filename
            filename = optarg;
            break;
        case 'h': // help
            fprintf(stdout, "%s", USAGE);
            exit(0);
            break;
        }
    }

    if (NULL == hostname)
    {
        fprintf(stderr, "%s @ %d: invalid host name\n", __FILE__, __LINE__);
        exit(1);
    }

    if (NULL == filename)
    {
        fprintf(stderr, "%s @ %d: invalid filename\n", __FILE__, __LINE__);
        exit(1);
    }

    if ((portno < 1025) || (portno > 65535))
    {
        fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__, portno);
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

    FILE *fp;

    fp = fopen(filename, "a");

    //send(client_socket, message, BUFSIZE, 0);

    char server_response[BUFSIZE];

    int x;

    while(1)
    {
        x = read(client_socket, &server_response, sizeof(server_response)-1);

        if(x == 0) break;

        //fputs(server_response, fp);

        fwrite(server_response, 1, x, fp);

        //printf("%s\n", server_response);


    }
    
    fclose(fp);

    close(client_socket);

    return 0;

}