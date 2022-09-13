#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;
using dfs_service::FileFile;
using dfs_service::CommandChannel;
using dfs_service::C_S_Command;
using dfs_service::S_C_Command;
using dfs_service::NULL_Parameter;
using dfs_service::FileStat;

using dfs_service::FileRequest;
using dfs_service::FileList;


//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = FileRequest;
using FileListResponseType = FileList;

extern dfs_log_level_e DFS_LOG_LEVEL;

std::mutex gl_mutex;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    //Custom
    std::map<std::string, std::string> client_map;
    std::map<std::string, std::unique_ptr<std::shared_timed_mutex>> file_mutex_map;
    std::mutex client_map_mutex;


    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* file_list)
    //void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response)
    {
        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        gl_mutex.lock();

        //std::cout << "Raman - This is the ProcessCallback" << std::endl;

        DIR *dr;
        struct dirent *en;
        struct stat result;
        dr = opendir(mount_path.c_str());
        std::string filename;
        int size;
        int mtime;
        int ctime;
        int cksum;
        std::string filepath;

        while ((en = readdir(dr)) != NULL)
        {
            filename = en->d_name;
            filepath = WrapPath(filename);

            if((stat(filepath.c_str(), &result)==0))
            {
                if(result.st_mode & S_IFREG)
                {
                    mtime = result.st_mtime;
                    ctime = result.st_ctime;
                    size = result.st_size;
                    cksum = dfs_file_checksum(filepath, &this->crc_table);

                    FileStat* file_stat = file_list->add_filestat();

                    file_stat->set_status(0);
                    file_stat->set_filename(filename);
                    file_stat->set_size(size);
                    file_stat->set_mtime(mtime);
                    file_stat->set_ctime(ctime);
                    file_stat->set_cksum(cksum);
                }
            }
        }
        
        closedir(dr);

        gl_mutex.unlock();
        //return Status::OK; 

    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //

    Status GetWriteLock(ServerContext* context, const CommandChannel* file_command, S_C_Command* server_response) override
    {
        std::string filename = file_command->filename();

        std::cout << "File is - " << filename << std::endl;

        std::string client_id = file_command->client_id();

        client_map_mutex.lock();

        if(client_map.count(filename) > 0)
        {
            if(client_map[filename] == client_id)
            {
                client_map_mutex.unlock();
                std::cout << "Client already has lock!" << std::endl;
                return Status(StatusCode::OK, "Lock already with client");
            }
            else
            {
                client_map_mutex.unlock();
                std::cout << "File already locked!" << std::endl;
                return Status(StatusCode::RESOURCE_EXHAUSTED, "Lock not available");
            }
        }
        else
        {
            //std::unique_ptr<std::mutex> file_mutex;
            file_mutex_map[filename] = std::make_unique<std::shared_timed_mutex>();
            //file_mutex_map.emplace(filename, &std::make_unique<std::mutex>());
            client_map[filename] = client_id;
            client_map_mutex.unlock();
            std::cout << "Lock given to client!" << std::endl;
            return Status(StatusCode::OK, "Lock received");
        }
    }



    Status StoreFile(ServerContext* context, ServerReader<FileFile>* reader, CommandChannel* file_command) override
    {
        FileFile file_ctx;
        std::ofstream FILE;

        reader->Read(&file_ctx);

        std::string filename =  file_ctx.filename();
        int filesize = file_ctx.size();
        int size = 0;

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        client_map_mutex.lock();

        if(client_map.count(filename) == 0)
        {
            std::cout << "No lock!" << std::endl;
            client_map_mutex.unlock();
            return Status(StatusCode::RESOURCE_EXHAUSTED, "No lock!");
        }

        if(client_map[filename] != file_ctx.client_id())
        {
            std::cout << "Not the same client that has lock!" << std::endl;
            client_map_mutex.unlock();
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Not the same client that has lock!");

        }

        auto temp = file_mutex_map.find(filename);
        
        std::shared_timed_mutex* file_mutex = temp->second.get();

        client_map_mutex.unlock();

        file_mutex->lock();
        struct stat f;

        if(stat (filepath.c_str(), &f) == 0)
        {
            std::cout << "File exists in server!" << std::endl;
            
            if(file_ctx.checksum() == dfs_file_checksum(filepath, &this->crc_table))
            {
                std::cout << "Exact file already exists in server!" << std::endl;
                file_mutex->unlock();

                client_map_mutex.lock();
                client_map.erase(filename);
                file_mutex_map.erase(filename);
                client_map_mutex.unlock();

                return Status(StatusCode::ALREADY_EXISTS, "Exact file already exists");
            }
            else if (file_ctx.mtime() < f.st_mtime)
            {
                std::cout << "More recent file exists in server!" << std::endl;
                file_mutex->unlock();

                client_map_mutex.lock();
                client_map.erase(filename);
                file_mutex_map.erase(filename);
                client_map_mutex.unlock();

                return Status(StatusCode::ALREADY_EXISTS, "More recent file already exists");
            }
        }

        FILE.open(filepath);

        while (reader->Read(&file_ctx))
        {
            if (context->IsCancelled())
            {
                std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
                FILE.close();

                file_mutex->unlock();

                client_map_mutex.lock();
                client_map.erase(filename);
                file_mutex_map.erase(filename);
                client_map_mutex.unlock();

                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
            }

            size += file_ctx.size();
            
            FILE << file_ctx.file_chunk();
        }
        FILE.close();
        file_mutex->unlock();

        client_map_mutex.lock();
        client_map.erase(filename);
        file_mutex_map.erase(filename);
        client_map_mutex.unlock();

        std::cout << "Received " << size << " off " <<  filesize << std::endl;

        if (size != filesize)
        {
            std::cout << "Not all file received!" << std::endl;
            return Status(StatusCode::CANCELLED, "Not all file received");
        }

        return Status::OK; 
    }


    Status FetchFile(ServerContext* context, const CommandChannel* file_command, ServerWriter<FileFile>* writer) override
    {
        std::string filename = file_command->filename();

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        std::cout << "File is at - " << filepath << std::endl;

        FileFile file_ctx;

        struct stat f;

        if(stat (filepath.c_str(), &f) != 0)
        {
            std::cout << "File not found!" << std::endl;
            file_ctx.set_status(1);
            writer->Write(file_ctx);
            return Status(StatusCode::NOT_FOUND, "File Not Found");
        }
        else
        {
            client_map_mutex.lock();
            if(client_map.count(filename) != 0)
            {
                client_map_mutex.unlock();
                std::cout << "File being written!" << std::endl;
                file_ctx.set_status(3);
                writer->Write(file_ctx);
                return Status(StatusCode::CANCELLED, "File being written");
            }
            else
            {
                if(file_command->checksum() == dfs_file_checksum(filepath, &this->crc_table))
                {
                    client_map_mutex.unlock();
                    std::cout << "File are same!" << std::endl;
                    file_ctx.set_status(2);
                    writer->Write(file_ctx);
                    return Status(StatusCode::ALREADY_EXISTS, "File are same");
                }
                else if(file_command->mtime() > f.st_mtime)
                {
                    client_map_mutex.unlock();
                    std::cout << "More recent file on client!" << std::endl;
                    file_ctx.set_status(4);
                    writer->Write(file_ctx);
                    return Status(StatusCode::ALREADY_EXISTS, "File are same");
                }
                
            }
            
        }

        client_map_mutex.unlock();
        
        file_ctx.set_status(0);

        int filesize = f.st_size;

        file_ctx.set_size(filesize);    

        writer->Write(file_ctx);

        std::ifstream FILE(filepath);

        int sent_bytes = 0;
        int read_bytes = 0;

        int BUFFER_SIZE = 1000;

        char buffer[BUFFER_SIZE];

        while(!FILE.eof())
        {

            if (context->IsCancelled())
            {
                FILE.close();
                std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
            }

            file_ctx.clear_file_chunk();

            read_bytes = std::min(filesize - sent_bytes, BUFFER_SIZE);

            FILE.read(buffer, BUFFER_SIZE);

            file_ctx.set_file_chunk(buffer, read_bytes);

            file_ctx.set_size(read_bytes);

            writer->Write(file_ctx);

            sent_bytes += read_bytes;

        }
        FILE.close();

        std::cout <<"Sent "<< sent_bytes << " off " <<  filesize <<std::endl;

        if (sent_bytes != filesize)
        {
            std::cout << "Not all file sent!" << std::endl;
            return Status(StatusCode::CANCELLED, "Not all file sent");
        }

        return Status::OK; 
    }


    Status DeleteFile(ServerContext* context, const CommandChannel* file_command, S_C_Command* server_response) override
    {
        std::string filename = file_command->filename();

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        std::cout << "File is at - " << filepath << std::endl;

        client_map_mutex.lock();

        if(client_map.count(filename) == 0)
        {
            std::cout << "No lock!" << std::endl;
            client_map_mutex.unlock();
            return Status(StatusCode::RESOURCE_EXHAUSTED, "No lock!");
        }

        if(client_map[filename] != file_command->client_id())
        {
            std::cout << "Not the same client that has lock!" << std::endl;
            client_map_mutex.unlock();
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Not the same client that has lock!");

        }

        auto temp = file_mutex_map.find(filename);
        
        std::shared_timed_mutex* file_mutex = temp->second.get();

        client_map_mutex.unlock();

        file_mutex->lock();
        struct stat f;

        if(stat (filepath.c_str(), &f) != 0)
        {
            file_mutex->unlock();
            client_map_mutex.lock();
            client_map.erase(filename);
            file_mutex_map.erase(filename);
            client_map_mutex.unlock();

            std::cout << "File not found!" << std::endl;
            server_response->set_command(1);
            return Status(StatusCode::NOT_FOUND, "File Not Found");
        }

        if (context->IsCancelled())
        {
            file_mutex->unlock();
            client_map_mutex.lock();
            client_map.erase(filename);
            file_mutex_map.erase(filename);
            client_map_mutex.unlock();

            std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
        }

        if(std::remove(filepath.c_str()) != 0)
        {
            file_mutex->unlock();
            client_map_mutex.lock();
            client_map.erase(filename);
            file_mutex_map.erase(filename);
            client_map_mutex.unlock();

            server_response->set_command(2);
            return Status(StatusCode::CANCELLED, "Could not delete file");
        }
        else
        {
            file_mutex->unlock();
            client_map_mutex.lock();
            client_map.erase(filename);
            file_mutex_map.erase(filename);
            client_map_mutex.unlock();

            server_response->set_command(0);
            return Status::OK;
        }
        
    }


    Status ListFile(ServerContext* context, const NULL_Parameter* null_parameter, FileList* file_list) override
    {
        DIR *dr;
        struct dirent *en;
        struct stat result;
        dr = opendir(mount_path.c_str());
        std::string filename;
        int size;
        int mtime;
        int ctime;
        int cksum;
        std::string filepath;

        if(!dr)
        {
            std::cout << "Directory not found!" << std::endl;
            return Status(StatusCode::NOT_FOUND, "Directory Not Found");
        }

        while ((en = readdir(dr)) != NULL)
        {
            if (context->IsCancelled())
            {
                std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
            }

            filename = en->d_name;
            filepath = WrapPath(filename);

            if((stat(filepath.c_str(), &result)==0))
            {
                if(result.st_mode & S_IFREG)
                {
                    mtime = result.st_mtime;
                    ctime = result.st_ctime;
                    size = result.st_size;
                    cksum = dfs_file_checksum(filepath, &this->crc_table);

                    std::cout << filename << "-" << mtime << std::endl;

                    FileStat* file_stat = file_list->add_filestat();

                    file_stat->set_status(0);
                    file_stat->set_filename(filename);
                    file_stat->set_size(size);
                    file_stat->set_mtime(mtime);
                    file_stat->set_ctime(ctime);
                    file_stat->set_cksum(cksum);
                }
            }
        }
        
        closedir(dr);
        
        return Status::OK; 

    }


    Status FileStats(ServerContext* context, const FileRequest* file_request, FileStat* file_stat) override
    {
        std::string filename = file_request->name();

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        std::cout << "File is at - " << filepath << std::endl;

        struct stat f;

        if(stat(filepath.c_str(), &f) != 0)
        {
            std::cout << "File not found!" << std::endl;
            file_stat->set_status(1);
            return Status(StatusCode::NOT_FOUND, "File Not Found");
        }

        if(context->IsCancelled())
        {
            std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
        }

        file_stat->set_status(0);
        file_stat->set_filename(filename);
        file_stat->set_size(f.st_size);
        file_stat->set_mtime(f.st_mtime);
        file_stat->set_ctime(f.st_ctime);

        return Status::OK;
        
    }


    Status CallbackList(ServerContext* context, const FileRequest* request, FileList* file_list) override
    {
        DIR *dr;
        struct dirent *en;
        struct stat result;
        dr = opendir(mount_path.c_str());
        std::string filename;
        int size;
        int mtime;
        int ctime;
        int cksum;
        std::string filepath;

        if(!dr)
        {
            std::cout << "Directory not found!" << std::endl;
            return Status(StatusCode::NOT_FOUND, "Directory Not Found");
        }

        while ((en = readdir(dr)) != NULL)
        {
            if (context->IsCancelled())
            {
                std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
            }

            filename = en->d_name;
            filepath = WrapPath(filename);

            if((stat(filepath.c_str(), &result)==0))
            {
                if(result.st_mode & S_IFREG)
                {
                    mtime = result.st_mtime;
                    ctime = result.st_ctime;
                    size = result.st_size;
                    cksum = dfs_file_checksum(filepath, &this->crc_table);

                    std::cout << filename << "-" << mtime << std::endl;

                    FileStat* file_stat = file_list->add_filestat();

                    file_stat->set_status(0);
                    file_stat->set_filename(filename);
                    file_stat->set_size(size);
                    file_stat->set_mtime(mtime);
                    file_stat->set_ctime(ctime);
                    file_stat->set_cksum(cksum);
                }
            }
        }
        
        closedir(dr);
        
        return Status::OK; 

    }



};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
