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

#define BUFSIZE 820

#define USAGE                                                \
    "usage:\n"                                               \
    "  transferserver [options]\n"                           \
    "options:\n"                                             \
    "  -f                  Filename (Default: 6200.txt)\n" \
    "  -h                  Show this help message\n"         \
    "  -p                  Port (Default: 20801)\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"filename", required_argument, NULL, 'f'},
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
    {NULL, 0, NULL, 0}};

int main(int argc, char **argv)
{
    int option_char;
    int portno = 20801;             /* port to listen on */
    char *filename = "6200.txt"; /* file to transfer */

    setbuf(stdout, NULL); // disable buffering

    // Parse and set command line arguments
    while ((option_char = getopt_long(argc, argv, "xp:hf:", gLongOptions, NULL)) != -1)
    {
        switch (option_char)
        {
        case 'p': // listen-port
            portno = atoi(optarg);
            break;
        default:
            fprintf(stderr, "%s", USAGE);
            exit(1);
        case 'h': // help
            fprintf(stdout, "%s", USAGE);
            exit(0);
            break;
        case 'f': // file to transfer
            filename = optarg;
            break;
        }
    }


    if ((portno < 1025) || (portno > 65535))
    {
        fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__, portno);
        exit(1);
    }
    
    if (NULL == filename)
    {
        fprintf(stderr, "%s @ %d: invalid filename\n", __FILE__, __LINE__);
        exit(1);
    }

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

    FILE *fp;

    int maxnpending = 5;

    while(listen(server_socket, maxnpending) == 0)
    {

        fp=fopen(filename, "r");

        int client_socket;

        client_socket = accept(server_socket, NULL, NULL);

        char ch[BUFSIZE];

        char *chp = ch;

        while(fgets(chp, BUFSIZE, fp) != NULL)
        {
        int delta = 0;

        while(1)
        {
                size_t length = strlen(chp);

                ssize_t sent = write(client_socket, chp, length);

                if(length == sent)
                {
                    chp -= delta;

                    break;
                }

                chp += sent;

                delta += sent;   

        }

        }


        fclose(fp);

        close(client_socket);

    }

    //close(server_socket);

    return 0;
}


/*
    while (fgets(chp, 12, fp) != NULL)
    {
        size_t length = strlen(chp);

        ssize_t sent = write(client_socket, chp, length);

        printf("%s\n", chp);
        
        printf("%lu\n", length);

    }
*/

/*
int maxnpending = 5;

    listen(server_socket, maxnpending);

    fp=fopen(filename, "r");

    int client_socket;

    client_socket = accept(server_socket, NULL, NULL);

    char ch[BUFSIZE];

    char *chp = ch;

    while(fgets(chp, BUFSIZE, fp) != NULL)
    {
      int delta = 0;

      while(1)
      {
            size_t length = strlen(chp);

            ssize_t sent = write(client_socket, chp, length);

            if(length == sent)
            {
                chp -= delta;

                break;
            }

            chp += sent;

            delta += sent;   

      }

    }


    fclose(fp);

    close(client_socket);

*/