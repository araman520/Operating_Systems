/*
 *  This file is for use by students to define anything they wish.  It is used by the proxy cache implementation
 */
 #ifndef __CACHE_STUDENT_H__
 #define __CACHE_STUDENT_H__

 #include "steque.h"

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

#include<stdio.h>
#include<sys/ipc.h>
#include<sys/shm.h>
#include<sys/types.h>
#include<string.h>
#include<errno.h>
#include<stdlib.h>
#include <semaphore.h>

//Used for cache dedicated request message queue
//And for sync message queues for the Proxy command channel
struct mesg_buffer { 
    long mesg_type; 
    key_t mesg_text; 
};

//Proxy command channel
struct shmseg {
    key_t skey;//command channel key
    key_t mkey;//data channel key
    char mesg_path[100];//request path
    size_t mesg_seg_size;//segment size
    size_t mesg_size;//size of the file chunck in shared memory
    size_t file_size;//total file size
    int mesg_control;//control variable to communicate file exists and if all file was sent
    struct mesg_buffer synt;//Proxy to Cache sync message queue structure
    struct mesg_buffer synr;//Cache to Proxy sync message queue structure
    int syn_msgid_tx;//Proxy to Cache sync message queue id
    int syn_msgid_rx;//Cache to Proxy sync message queue id

};
 
 #endif // __CACHE_STUDENT_H__