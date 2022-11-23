#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using masterworker::worker;
using masterworker::workRequest;
using masterworker::File;
using masterworker::workResponse;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		std::string ip_addr_port;

		class CallData {
			public:
				CallData(worker::AsyncService* service, ServerCompletionQueue* cq)
					: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
					Proceed();
				}

				void Proceed() {
					if (status_ == CREATE) {
						status_ = PROCESS;

						service_->RequestrequestWork(&ctx_, &request_, &responder_, cq_, cq_, this);
					} else if (status_ == PROCESS) {
						new CallData(service_, cq_);

						std::vector<File> shard;
						for (auto file : request_.shard()) {
							shard.push_back(file);
						}
						if (request_.requesttype() == "MAP") {
							auto mapper = get_mapper_from_task_factory(request_.userid());
							mapper->impl_->outputDir = request_.outputdir();
							mapper->impl_->rPartitions = request_.rpartition();
							map(shard, mapper);
							for (auto path : get_mapper_output_paths(mapper)) {
								reply_.add_outputfile(path);
							}
						} else {
							auto reducer = get_reducer_from_task_factory(request_.userid());
							reducer->impl_->outputDir = request_.outputdir();
							reduce(shard, reducer);
							reply_.add_outputfile(get_reducer_output_path(reducer));
						}

						reply_.set_status(true);
						status_ = FINISH;
						responder_.Finish(reply_, Status::OK, this);
					} else {
						GPR_ASSERT(status_ == FINISH);
						delete this;
					}
				}

			private:
				worker::AsyncService* service_;
				ServerCompletionQueue* cq_;
				ServerContext ctx_;

				workRequest request_;
				workResponse reply_;

				ServerAsyncResponseWriter<workResponse> responder_;

				enum CallStatus { CREATE, PROCESS, FINISH };
				CallStatus status_;

				std::vector<std::string> get_mapper_output_paths(std::shared_ptr<BaseMapper> mapper) {
					return mapper->impl_->getListFiles();
				}

				std::string get_reducer_output_path(std::shared_ptr<BaseReducer> reducer) {
					return reducer->impl_->outputDir + "/" + reducer->impl_->outputFile;
				}


				// Snippet from Stack Overflow https://stackoverflow.com/a/55263720
				std::vector<std::string> split_str(std::string str) {
					std::string tmp; 
					std::stringstream ss(str);
					std::vector<std::string> result;

					while (std::getline(ss, tmp, ' ')){
						result.push_back(tmp);
					}
					return result;
				}

				void map(std::vector<File> shard, std::shared_ptr<BaseMapper> mapper) {
					std::ifstream in_file;	
					std::string line;
					std::cout << "Starting mapping" << std::endl;
					for (auto file : shard) {
						std::cout << "Mapping " << file.filepath() << std::endl;
						size_t current_offset = file.startoffset();
						in_file.open(file.filepath(), std::ios::binary);
						in_file.seekg(file.startoffset());
						while (current_offset < file.endoffset()) {
							std::getline(in_file, line);
							current_offset += line.size() + 1;
							mapper->map(line);
						}
						std::cout << "Done mapping " << file.filepath() << std::endl;
					}
					return;
				}

				void reduce(std::vector<File> shard, std::shared_ptr<BaseReducer> reducer) {
					std::cout << "Starting reducing" << std::endl;
					std::ifstream in_file;
					std::string line;
					for (auto file : shard) {
						in_file.open(file.filepath(), std::ios::in);	
						std::cout << "Reducing " << file.filepath() << std::endl;
						while (!in_file.eof()) {
							std::getline(in_file, line);
							std::istringstream is_line(line);
							std::string key;
							if (std::getline(is_line, key, ' ')) {
								std::string value;
								if (std::getline(is_line, value)) 
									reducer->reduce(key, split_str(value));
							}
						}
						in_file.clear();
						in_file.close();
					}
					std::cout << "Done reducing" << std::endl;
				}
		};

		void HandleRpcs() {
			new CallData(&service_, cq_.get());
			void* tag;
			bool ok;
			while (true) {
				GPR_ASSERT(cq_->Next(&tag, &ok));
				GPR_ASSERT(ok);
				static_cast<CallData*>(tag)->Proceed();
			}
		}

	std::unique_ptr<ServerCompletionQueue> cq_;
	worker::AsyncService service_;
	std::unique_ptr<Server> server_;
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	this->ip_addr_port = ip_addr_port;
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	std::string server_address(this->ip_addr_port);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    HandleRpcs();

	return true;
}
