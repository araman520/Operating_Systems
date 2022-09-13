
#include "gfserver-student.h"

/* 
 * Modify this file to implement the interface specified in
 * gfserver.h.
 */


struct gfserver_t
    {
        int server_socket;
        int max_pending;
        unsigned short listening_port;
        gfh_error_t (*getfile_handler_function)();
        void* handler_arg;
    };


struct gfcontext_t
    {
        int client_socket;
        char* request_method;
        char* request_header;
        const char* request_file_path;
        gfstatus_t status;
    };



void gfs_abort(gfcontext_t **ctx)
{
    gfcontext_t *ct2 = *ctx;
    close(ct2->client_socket);
}

gfserver_t* gfserver_create()
{
    gfserver_t *gfs = malloc(sizeof *gfs); //Allocate memory

    return gfs;
}

ssize_t gfs_send(gfcontext_t **ctx, const void *data, size_t len)
{
    gfcontext_t *ct2 = *ctx;
    ssize_t ds = 0;

    //Send until all the data is sent
    while(ds != len) ds += send(ct2->client_socket, data+ds, len-ds, 0);

    return ds;
}

ssize_t gfs_sendheader(gfcontext_t **ctx, gfstatus_t status, size_t file_len)
{
    gfcontext_t *ct2 = *ctx;

    //Build response header
    char response_scheme[] = "GETFILE"; //Response scheme
    char *response_status;

    //Response Method
    if(status == GF_OK) response_status = "OK";
    else if(status == GF_FILE_NOT_FOUND) response_status = "FILE_NOT_FOUND";
    else if(status == GF_INVALID) response_status = "INVALID";
    else response_status = "ERROR";

    char response_header[100];

    //Final header to be sent
    if(status == GF_OK) snprintf(response_header, sizeof(response_header),  "%s %s %li\r\n\r\n", response_scheme, response_status, file_len);
    else snprintf(response_header, sizeof(response_header),  "%s %s\r\n\r\n", response_scheme, response_status);
    
    printf("Header to be sent: %s\n", response_header);
    
    //Send header
    ssize_t hs = send(ct2->client_socket, response_header, strlen(response_header), 0);

    return hs;

}

void gfserver_serve(gfserver_t **gfs)
{
    gfserver_t* gf2 = *gfs;

    //Get address to bind
    struct addrinfo hints, *results, *record;

    char port_string[10];
    sprintf(port_string, "%d", gf2->listening_port); //Port string for getaddrinfo function

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC; //IP Agnostic
    hints.ai_socktype = SOCK_STREAM; //TCP

    char *hostname = "127.0.0.1";

    if (getaddrinfo(hostname, port_string, &hints, &results) != 0) 
    {
        perror("Failed to translate client socket.");
        exit(EXIT_FAILURE);
    }

    // loop through all the results and bind to the first we can

    int enable = 1;

    for(record = results; record != NULL; record = record->ai_next) {
        gf2->server_socket = socket(record->ai_family, record->ai_socktype, record->ai_protocol);

        if(gf2->server_socket == -1) continue;

        setsockopt(gf2->server_socket, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

        if (bind(gf2->server_socket, record->ai_addr, record->ai_addrlen) != -1) break;

        close(gf2->server_socket);
    }

    freeaddrinfo(results);

    char delimeter[] = "\r\n\r\n"; //End string for header

    //Server clients

    while(listen(gf2->server_socket, gf2->max_pending) == 0)
    {
        gfcontext_t *ct2 = malloc(sizeof *ct2);

        ct2->client_socket = accept(gf2->server_socket, NULL, NULL);

        printf("Client Socket: %d\n", ct2->client_socket);


        //Receive request header
        char client_request[100] = "";
        char client_request_temp[100] = "";

        int found_header = 0;
        int start = 0;
        int hl = 0;
        int x;

        while(1)
        {
            x = read(ct2->client_socket, &client_request, 1);
            if(x==1) start = 1;
            if((hl >= 95) || ((start == 1) && (x == 0))) break;

            if((start==1) && (x == 1))
            {
                strncat(client_request_temp, client_request, 1);
                
                if(strstr(client_request_temp, delimeter))
                {
                    found_header = 1;
                    break;
                }
            }
            hl += x;
        }

        printf("%s\n", client_request_temp);

        ct2->status = GF_OK;

        if(found_header == 0)
        {
            printf("Header not found!\n"); //If header not found
            ct2->status = GF_INVALID;
        }
        else
        {
            printf("Header found!\n"); //Header found

            if(!(strstr(client_request_temp, "GETFILE"))) //Check scheme
            {
                printf("Wrong Scheme!\n");
                ct2->status = GF_INVALID;
            }
            else
            {
                //If scheme is correct, parse to get method and file path
                char* head_head = strstr(client_request_temp, "GETFILE");

                int i = 0;
                ct2->request_header = strtok(head_head, delimeter);

                char *q = strtok(ct2->request_header, " ");
                char *array_2[3];

                while (q != NULL)
                {
                    array_2[i++] = q;
                    q = strtok(NULL, " ");
                }

                ct2->request_method = array_2[1]; //Request Method
                ct2->request_file_path = array_2[2]; //Request File path

                if(!(strstr(ct2->request_method, "GET"))) //Check request method
                {
                    printf("Wrong Method!\n");
                    ct2->status = GF_INVALID;
                }
                else
                {
                    if(ct2->request_file_path[0] != '/') //Check if valid file path
                    {
                        printf("Invalid Filename!\n");
                        ct2->status = GF_INVALID;
                    } 
                }                  
            }
        }

        if(ct2->status != GF_OK)
        {
            gfs_sendheader(&ct2, ct2->status, 0); //Send appropriate header if status not OK
        }
        else
        {
            //Call handler to check if file exists and send the file if it does
            gf2->getfile_handler_function(&ct2, ct2->request_file_path, gf2->handler_arg);
            
        }
        
        printf("DONE!\n");
    }

}

void gfserver_set_handlerarg(gfserver_t **gfs, void* arg)
{
    gfserver_t *gf2 = *gfs;

    gf2->handler_arg = arg;
}

void gfserver_set_handler(gfserver_t **gfs, gfh_error_t (*handler)(gfcontext_t **, const char *, void*))
{
    gfserver_t *gf2 = *gfs;

    gf2->getfile_handler_function = handler;

}

void gfserver_set_maxpending(gfserver_t **gfs, int max_npending)
{
    gfserver_t *gf2 = *gfs;

    gf2->max_pending = max_npending;

}

void gfserver_set_port(gfserver_t **gfs, unsigned short port)
{
    gfserver_t *gf2 = *gfs;

    gf2->listening_port = port;
}


