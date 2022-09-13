#include <regex>
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
#include <sys/stat.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;
using dfs_service::FileFile;
using dfs_service::CommandChannel;
using dfs_service::C_S_Command;
using dfs_service::S_C_Command;
using dfs_service::DFSService;
using dfs_service::NULL_Parameter;
using dfs_service::FileStat;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

//StatusCode DFSClientNodeP1::Store(const std::string& user)
StatusCode DFSClientNodeP1::Store(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
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
    else 
    {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return StatusCode::CANCELLED;
    }

}


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
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

    std::string filepath = WrapPath(filename);

    std::cout << "File is at - " << filepath << std::endl;

    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    CommandChannel file_command;

    FileFile file_ctx;

    file_ctx.set_filename(filename);

    file_command.set_filename(filename);

    std::unique_ptr<ClientReader<FileFile>> reader(service_stub->FetchFile(&context, file_command));

    reader->Read(&file_ctx);

    if(file_ctx.status() != 0)
    {
        std::cout << "File not found - " << filename << std::endl;
        return StatusCode::NOT_FOUND;
    }

    std::ofstream FILE;
    //std::string filename_2 =  filename + "_2";
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

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
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

    CommandChannel file_command;
    S_C_Command server_respose;

    file_command.set_filename(filename);

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
    else 
    {
        if(server_respose.command() == 1)
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

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
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

    CommandChannel file_command;
    NULL_Parameter null_parameter;

    std::unique_ptr<ClientReader<CommandChannel>> reader(service_stub->ListFile(&context, null_parameter));

    //reader->Read(&file_ctx);

    try
    {
        while (reader->Read(&file_command))
        {
            if (file_command.status() == 1)
            {
                std::cout << "Could not list files!" << std::endl;
                return StatusCode::CANCELLED;
            }

            std::cout << file_command.filename() << "-" << file_command.time() << std::endl;
            
            file_map->insert(std::pair<std::string, int>(file_command.filename(), file_command.time()));
        }
    }
    catch(...)
    {
        std::cout << "Directory error!" <<std::endl;
        return StatusCode::CANCELLED;
    }


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

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need  
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
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

    CommandChannel file_command;
    FileStat file_stats;

    file_command.set_filename(filename);

    Status status = service_stub->FileStats(&context, file_command, &file_stats);

    if (status.ok())
    {
        file_status = (void*)&file_stats;
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
        if(file_stats.status() == 1)
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

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//

