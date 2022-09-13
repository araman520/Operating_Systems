#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using dfs_service::DFSService;
using dfs_service::FileFile;
using dfs_service::CommandChannel;
using dfs_service::C_S_Command;
using dfs_service::S_C_Command;
using dfs_service::NULL_Parameter;
using dfs_service::FileStat;
using dfs_service::FileRequest;
using dfs_service::FileList;

std::mutex global_mutex;


extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = FileRequest;
using FileListResponseType = FileList;


DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    CommandChannel file_command;

    file_command.set_filename(filename);

    file_command.set_client_id(client_id);

    S_C_Command server_respose;

    ClientContext context;

    Status status = service_stub->GetWriteLock(&context, file_command, &server_respose);

    if (status.ok())
    {
      return StatusCode::OK;
    } 
    else if(status.error_code() == 4)
    {
      std::cout << "Deadline exceeded!" << std::endl;
      return StatusCode::DEADLINE_EXCEEDED;
    }
    else if(status.error_code() == 8)
    {
      std::cout << "Resource exhausted!" << std::endl;
      return StatusCode::RESOURCE_EXHAUSTED;
    }
    else 
    {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return StatusCode::CANCELLED;
    }

}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    std::cout << "File is - " << filename << std::endl;

    std::string filepath = WrapPath(filename);

    std::cout << "File is at - " << filepath << std::endl;

    struct stat f;

    if(stat (filepath.c_str(), &f) != 0)
    {
        std::cout << "File not found!" << std::endl;
        return StatusCode::NOT_FOUND;
    }

    int filesize = f.st_size;

    std::cout << "File Size - " << filesize <<std::endl;
    
    
    StatusCode status_code = RequestWriteAccess(filename);

    if (status_code != StatusCode::OK)
    {
        std::cout << "Could not get lock!" << std::endl;
        return status_code;
    }

    ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    CommandChannel file_command;

    std::unique_ptr<ClientWriter<FileFile>> writer(service_stub->StoreFile(&context, &file_command));

    FileFile file_ctx;

    std::ifstream FILE(filepath);

    int sent_bytes = 0;
    int read_bytes = 0;

    int BUFFER_SIZE = 1000;

    char buffer[BUFFER_SIZE];

    file_ctx.set_filename(filename);
    file_ctx.set_size(filesize);
    file_ctx.set_client_id(client_id);
    file_ctx.set_checksum(dfs_file_checksum(filepath, &this->crc_table));
    file_ctx.set_mtime(f.st_mtime);
    writer->Write(file_ctx);

    try
    {
        while(!FILE.eof())
        {
            file_ctx.clear_file_chunk();

            read_bytes = std::min(filesize - sent_bytes, BUFFER_SIZE);

            FILE.read(buffer, BUFFER_SIZE);

            file_ctx.set_file_chunk(buffer, read_bytes);

            file_ctx.set_size(read_bytes);

            writer->Write(file_ctx);

            sent_bytes += read_bytes;
        }

    }
    catch(...)
    {
        if(sent_bytes != filesize)
        {
            std::cout << "File sent error" <<std::endl;
            FILE.close();
            return StatusCode::CANCELLED;
        }
    }

    std::cout <<"Sent "<< sent_bytes << " off " <<  filesize <<std::endl;
    
    FILE.close();

    writer->WritesDone();

    Status status = writer->Finish();

    if (status.ok())
    {
      return StatusCode::OK;
    }
    else if(status.error_code() == 4)
    {
      std::cout << "Deadline exceeded!" << std::endl;
      return StatusCode::DEADLINE_EXCEEDED;
    }
    else if(status.error_code() == 6)
    {
      std::cout << "File already exists!" << std::endl;
      return StatusCode::ALREADY_EXISTS;
    }
    else 
    {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return StatusCode::CANCELLED;
    }

}


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //

    std::cout << "File is - " << filename << std::endl;

    std::string filepath = WrapPath(filename);

    std::cout << "File is at - " << filepath << std::endl;

    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    CommandChannel file_command;

    FileFile file_ctx;

    file_command.set_filename(filename);

    struct stat f;

    if(stat (filepath.c_str(), &f) != 0)
    {
        std::cout << "File not found on cient!" << std::endl;
        file_command.set_status(1);
    }
    else
    {
        std::cout << "File exists on cient!" << std::endl;
        file_command.set_status(0);
        file_command.set_checksum(dfs_file_checksum(filepath, &this->crc_table));
        file_command.set_mtime(f.st_mtime);
    }
    

    std::unique_ptr<ClientReader<FileFile>> reader(service_stub->FetchFile(&context, file_command));

    reader->Read(&file_ctx);

    if(file_ctx.status() == 1)
    {
        std::cout << "File not found - " << filename << std::endl;
        return StatusCode::NOT_FOUND;
    }
    else if(file_ctx.status() == 2)
    {
        std::cout << "File same as server - " << filename << std::endl;
        return StatusCode::ALREADY_EXISTS;
    }
    else if(file_ctx.status() == 3)
    {
        std::cout << "File being written - " << filename << std::endl;
        return StatusCode::CANCELLED;
    }
    else if(file_ctx.status() == 4)
    {
        std::cout << "More recent file on client - " << filename << std::endl;
        return StatusCode::ALREADY_EXISTS;
    }

    std::ofstream FILE;
    int filesize = file_ctx.size();
    int size = 0;

    FILE.open(filepath);

    try
    {
        while (reader->Read(&file_ctx))
        {
            size += file_ctx.size();
            
            FILE << file_ctx.file_chunk();
        }
    }
    catch(...)
    {
        std::cout << "File Receive error" <<std::endl;
        FILE.close();
        return StatusCode::CANCELLED;
    }

    std::cout << "Received " << size << " off " <<  filesize << std::endl;

    FILE.close();

    Status status = reader->Finish();



    if (status.ok())
    {
      return StatusCode::OK;
    } 
    else if(status.error_code() == 4)
    {
      std::cout << "Deadline exceeded!" << std::endl;
      return StatusCode::DEADLINE_EXCEEDED;
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return StatusCode::CANCELLED;
    }

}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    std::cout << "File is - " << filename << std::endl;

    StatusCode status_code = RequestWriteAccess(filename);

    if (status_code != StatusCode::OK)
    {
        std::cout << "Could not get lock!" << std::endl;
        return status_code;
    }

    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    CommandChannel file_command;
    S_C_Command server_respose;

    file_command.set_filename(filename);
    file_command.set_client_id(client_id);

    Status status = service_stub->DeleteFile(&context, file_command, &server_respose);

    if (status.ok())
    {
      std::cout << "File Deleted - " << filename << std::endl;
      return StatusCode::OK;
    }
    else if(status.error_code() == 4)
    {
      std::cout << "Deadline exceeded!" << std::endl;
      return StatusCode::DEADLINE_EXCEEDED;
    }
    else if(status.error_code() == 5)
    {
      std::cout << "File not found!" << std::endl;
      return StatusCode::NOT_FOUND;
    }
    else
    {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return StatusCode::CANCELLED;
    }

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    FileList file_list;
    NULL_Parameter null_parameter;

    Status status = service_stub->ListFile(&context, null_parameter, &file_list);

    if (status.ok())
    {
        for (int j = 0; j < file_list.filestat_size(); j++)
        {
            const FileStat& file_stat = file_list.filestat(j);
            std::cout << file_stat.filename() << "-" << file_stat.mtime() << std::endl;
            file_map->insert(std::pair<std::string, int>(file_stat.filename(), file_stat.mtime()));
        }

        return StatusCode::OK;
    } 
    else if(status.error_code() == 4)
    {
        std::cout << "Deadline exceeded!" << std::endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else 
    {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return StatusCode::CANCELLED;
    }

}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    std::cout << "File is - " << filename << std::endl;

    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    FileRequest file_request;
    FileStat file_stat;

    file_request.set_name(filename);

    Status status = service_stub->FileStats(&context, file_request, &file_stat);

    if (status.ok())
    {
        file_status = (void*)&file_stat;
        std::cout << "File Stats received - " << filename << std::endl;
        return StatusCode::OK;
    }
    else if(status.error_code() == 4)
    {
      std::cout << "Deadline exceeded!" << std::endl;
      return StatusCode::DEADLINE_EXCEEDED;
    }
    else 
    {
        if(file_stat.status() == 1)
        {
            std::cout << status.error_code() << ": " << status.error_message() << std::endl;
            return StatusCode::NOT_FOUND;
        }
        else
        {
            std::cout << status.error_code() << ": " << status.error_message() << std::endl;
            return StatusCode::CANCELLED;
        }
        
    }


}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //
    global_mutex.lock();

    callback();

    global_mutex.unlock();

}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    std::cout << "Raman - This is the HandleCallbackList" << std::endl;

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //

                std::string server_filename;
                int server_mtime;
                int server_cksum;
                std::string client_filepath;

                int client_mtime;
                int client_cksum;

                global_mutex.lock();

                FileList server_file_list = call_data->reply;

                for (int j = 0; j < server_file_list.filestat_size(); j++)
                {
                    const FileStat& server_file_stat = server_file_list.filestat(j);

                    server_filename = server_file_stat.filename();
                    server_mtime = server_file_stat.mtime();
                    client_filepath = WrapPath(server_filename);
                    server_cksum = server_file_stat.cksum();

                    struct stat f;

                    if(stat (client_filepath.c_str(), &f) != 0) Fetch(server_filename);
                    else
                    {
                        client_mtime = f.st_mtime;
                        client_cksum = dfs_file_checksum(client_filepath, &this->crc_table);

                        if (client_cksum != server_cksum)
                        {
                            if (client_mtime > server_mtime) Store(server_filename);
                            else if (client_mtime < server_mtime) Fetch(server_filename);
                        }
                    }  
                }

                global_mutex.unlock();



            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//


