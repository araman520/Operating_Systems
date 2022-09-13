#include "gfclient-student.h"

typedef struct gfcrequest_t
    {
        char *file_server; //Server info
        char *file_path; //Path of the file required
        unsigned short client_port; //Port info
        void (*client_write_function)(); //function to write file
        char* file_length; //Length of file according to the received header
        size_t tx_bytes; //Actual transmitted bytes
        size_t rx_bytes; //Actual received bytes
        gfstatus_t status; //Status

        char* request_scheme;
        char* request_method;
        char* request_header;

        char* response_scheme;
        char* response_status;
        char* response_header;

        void* write_arg;

        void (*client_header_function)();
        void* header_arg;


    }gfcrequest_t;

// optional function for cleaup processing.
void gfc_cleanup(gfcrequest_t **gfr)
{
    free(*gfr);
    *gfr = NULL;
    gfr = NULL;

}

gfcrequest_t *gfc_create()
{

    gfcrequest_t *gfr = malloc(sizeof *gfr); //Allocate memory
    gfr->status = GF_OK; //Intialize status to OK

    return gfr;
}

size_t gfc_get_bytesreceived(gfcrequest_t **gfr)
{
    gfcrequest_t *gf2 = *gfr;

    return gf2->rx_bytes;
}

size_t gfc_get_filelen(gfcrequest_t **gfr)
{
    gfcrequest_t *gf2 = *gfr;

    return gf2->tx_bytes;
}

gfstatus_t gfc_get_status(gfcrequest_t **gfr)
{
    gfcrequest_t *gf2 = *gfr;

    return gf2->status;
}

void gfc_global_init(){}

void gfc_global_cleanup(){}

int gfc_perform(gfcrequest_t **gfr)
{
    gfcrequest_t *gf2 = *gfr;

    printf("Server:%s\n", gf2->file_server);

    if (strcmp(gf2->file_server, "localhost")==0) gf2->file_server = "127.0.0.1"; //local host ip

    char port_string[10];
    sprintf(port_string, "%d", gf2->client_port); //port info string for getaddrinfo function

    //Create results linked list containing addresses

    struct addrinfo hints, *results, *record;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(gf2->file_server, port_string, &hints, &results) != 0) 
    {
        perror("Failed to translate client socket.");
        exit(EXIT_FAILURE);
    }

    //Bind to the first address we can

    int client_socket;

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

    #define BUFSIZE 2012

    //Creating Header to send

    gf2->request_scheme = "GETFILE";
    gf2->request_method = "GET";
    char delimeter[] = "\r\n\r\n";

    char header[100]; 

    snprintf(header, sizeof(header),  "%s %s %s%s",gf2->request_scheme, gf2->request_method, gf2->file_path, delimeter);

    gf2->request_header = header; //Final header to send

    gf2->rx_bytes = 0;

    char server_response[50] = "";
    char server_response_temp[50] = "";

    printf("Header Sent:%s\n", gf2->request_header);

    write(client_socket, gf2->request_header, strlen(gf2->request_header)); //Send header

    int total_bytes_read = 0;
    int current_bytes_read = 0;
    int found_header = 0;

    int start = 0;
    int hl = 0;
    int x;

    //Receive response header
    while(1)
    {
        x = read(client_socket, &server_response, 1);
        if(x==1) start = 1;
        if((hl >= 45) || ((start == 1) && (x == 0))) break;

        if((start==1) && (x == 1))
        {
            strncat(server_response_temp, server_response, 1);
            
            if(strstr(server_response_temp, delimeter))
            {
                found_header = 1;
                break;
            }
        }
        hl += x;
    }

    printf("%s\n", server_response_temp);

    //If header not found
    if(found_header == 0)
    {
        printf("Header not found!\n");
        gf2->status = GF_INVALID;
        close(client_socket);
        return -1;
    }

    printf("Header found!\n");

    //Making sure response header has correct scheme
    if(!(strstr(server_response_temp, "GETFILE")))
    {
        printf("Wrong Scheme!\n");
        gf2->status = GF_INVALID;
        close(client_socket);
        return -1;
    }

    gf2->response_scheme = "GETFILE";

    //Parsing the rest of the header to get status and file length
    
    char* head_head = strstr(&server_response_temp, "GETFILE");

    int i = 0;
    gf2->response_header = strtok(head_head, delimeter);

    i = 0;
    char *q = strtok (gf2->response_header, " ");
    char *array_2[3];

    while (q != NULL)
    {
        array_2[i++] = q;
        q = strtok(NULL, " ");
    }

    char *header_status = "";

    header_status = array_2[1]; //Response status
    gf2->file_length = array_2[2]; //Response file length

    printf("1.rx_header:%s\n", gf2->response_header);
    printf("2.protocol_scheme:%s\n", gf2->response_scheme);
    printf("3.rx_status:%s\n", header_status);
    printf("4.rx_file_len:%s\n", gf2->file_length);

    //Checking if response status is one of {OK, FILE_NOT_FOUND, ERROR, INVALID}
    if((!(strstr(header_status, "OK"))) && (!(strstr(header_status, "FILE_NOT_FOUND"))) && (!(strstr(header_status, "ERROR"))) && (!(strstr(header_status, "INVALID"))))
    {
        printf("Wrong Status!\n");
        gf2->status = GF_INVALID;
        close(client_socket);
        return -1;   
    }

    //If server does not have file, exit
    if(strstr(header_status, "FILE_NOT_FOUND"))
    {
        printf("Status: File not found!\n");
        gf2->status = GF_FILE_NOT_FOUND;
        close(client_socket);
        return 0;
    }

    //If server sent an error, exit
    if(strstr(header_status, "ERROR"))
    {
        printf("Status: Error!\n");
        gf2->status = GF_ERROR;
        close(client_socket);
        return 0;
    }

    //If server thinks sent header is invalid, exit
    if(strstr(header_status, "INVALID"))
    {
        printf("Status: Invalid!\n");
        gf2->status = GF_INVALID;
        close(client_socket);
        return -1;
    }

    //Receive the file

    gf2->tx_bytes = atoi(gf2->file_length);

    printf("Tx bytes number = %zu\n", gf2->tx_bytes);

    x = 0;
    char server_response_2[2000];

    while(1)
    {
        gf2->rx_bytes += x;

        if(gf2->rx_bytes >= gf2->tx_bytes) break;

        x = recv(client_socket, &server_response_2, sizeof(server_response_2)-1, 0);

        if(x < 1) break;

        (gf2->client_write_function)(server_response_2, x, gf2->write_arg);


    }

    printf("%zu of %zu received\n", gf2->rx_bytes, gf2->tx_bytes);


    //If the server quit before sending all of the file
    if (gf2->tx_bytes > gf2->rx_bytes)
    {
        printf("Did not receive all bytes!\n");

        close(client_socket);

        return -1;

    }

    close(client_socket);

    return 0;

}

void gfc_set_headerarg(gfcrequest_t **gfr, void *headerarg)
{
    gfcrequest_t *gf2 = *gfr;
    gf2->header_arg = headerarg;
}

void gfc_set_headerfunc(gfcrequest_t **gfr, void (*headerfunc)(void*, size_t, void *))
{
    gfcrequest_t *gf2 = *gfr;
    gf2->client_header_function = headerfunc;
}

void gfc_set_path(gfcrequest_t **gfr, const char* path)
{
    gfcrequest_t *gf2 = *gfr;
    gf2->file_path = path;

}

void gfc_set_port(gfcrequest_t **gfr, unsigned short port)
{
    gfcrequest_t *gf2 = *gfr;
    gf2->client_port = port;

}

void gfc_set_server(gfcrequest_t **gfr, const char* server)
{
    gfcrequest_t *gf2 = *gfr;
    gf2->file_server = server;
  
}

void gfc_set_writearg(gfcrequest_t **gfr, void *writearg)
{
    gfcrequest_t *gf2 = *gfr;
    gf2->write_arg = writearg;

}

void gfc_set_writefunc(gfcrequest_t **gfr, void (*writefunc)(void*, size_t, void *))
{
    gfcrequest_t *gf2 = *gfr;
    gf2->client_write_function = writefunc;
  
}

const char* gfc_strstatus(gfstatus_t status){
    const char *strstatus = NULL;

    switch (status)
    {
        default: {
            strstatus = "UNKNOWN";
        }
        break;

        case GF_INVALID: {
            strstatus = "INVALID";
        }
        break;

        case GF_FILE_NOT_FOUND: {
            strstatus = "FILE_NOT_FOUND";
        }
        break;

        case GF_ERROR: {
            strstatus = "ERROR";
        }
        break;

        case GF_OK: {
            strstatus = "OK";
        }
        break;
        
    }

    return strstatus;
}