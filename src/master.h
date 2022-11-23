#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "chrono"
#include <memory>
#include <grpcpp/grpcpp.h>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using masterworker::workRequest;
using masterworker::workResponse;
using masterworker::File;
using masterworker::worker;


class mapreduceClient
{
	public:
		mapreduceClient(std::shared_ptr<Channel> channel){
			stub_ = worker::NewStub(channel);
		}
		
		workResponse requestWork(workRequest &request)
		{
			workResponse response;
			ClientContext context;
			std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
			context.set_deadline(deadline);
			Status status = stub_->requestWork(&context, request, &response);
			if (status.ok()) {
				return response;
			} else {
				std::cout << status.error_code() << ": " << status.error_message() << std::endl;
				response.set_status(false);
				return response;
			}
		}

	private:
		std::unique_ptr<worker::Stub> stub_;



};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master
{

public:
	/* DON'T change the function signature of this constructor */
	Master(const MapReduceSpec &, const std::vector<FileShard> &);

	/* DON'T change this function's signature */
	bool run();

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	MapReduceSpec mrSpec;
	std::vector<FileShard> fileShards; // vector of file shards
	std::vector<mapreduceClient*> workers; // vector of pointers to workers
	std::vector<workRequest> requests;
	std::vector<workResponse> responses;

	std::vector<int> mapStatus; // vector of shard map statuses
	std::vector<int> reduceStatus;

	enum workerStatus {IDLE, BUSY, DOWN};
	std::vector<workerStatus> workerStatuses;
	int availableWorkers;

	std::mutex nodeMutex;
	std::condition_variable nodeCV;
	std::vector<std::thread> nodeThreads;

	std::mutex intermediateFileMutex;
	std::vector<std::string> intermediateFiles;
	
	int getFreeWorker();
	void mapNode(int shardId);
	void reduceNode(int reduceId);
	void waitTillAllWorkersAreDone();




};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	mrSpec = mr_spec;
	fileShards = file_shards;

	for (int i = 0; i < mrSpec.worker_ipaddr_ports.size(); i++) {
		std::string ip = mrSpec.worker_ipaddr_ports[i];
		workers.push_back(new mapreduceClient(grpc::CreateChannel(ip, grpc::InsecureChannelCredentials())));
		workerStatuses.push_back(IDLE);
		availableWorkers++;
	}
	for (int i = 0; i < fileShards.size(); i++) {
		mapStatus.push_back(0);
	}

	for (size_t i = 0; i < mrSpec.n_output_files; i++)
	{
		reduceStatus.push_back(0);
	}
	
}
int Master::getFreeWorker(){
	int workerId = -1;
	std::unique_lock<std::mutex> lock(nodeMutex);
	nodeCV.wait(lock, [this] {return availableWorkers > 0;});
	for (int i = 0; i < workerStatuses.size(); i++)
	{
		if (workerStatuses[i] == IDLE)
		{
			workerId = i;
			break;
		}
	}
	workerStatuses[workerId] = BUSY;
	availableWorkers--;
	return workerId;
}

void Master::mapNode(int shardId){
	int freeWorker = getFreeWorker();
	workRequest *request;
	request->set_requesttype("MAP");
	request->set_userid(mrSpec.user_id);
	request->set_rpartition(mrSpec.n_workers);
	request->set_outputdir(mrSpec.output_dir);
	request->set_rpartition(mrSpec.n_output_files);
	for (size_t j = 0; j < fileShards[shardId].file_offsets.size(); j++)
	{
		File *newFile = request->add_shard();
		newFile->set_filepath(fileShards[shardId].file_offsets[j].filename);
		newFile->set_startoffset(fileShards[shardId].file_offsets[j].start);
		newFile->set_endoffset(fileShards[shardId].file_offsets[j].end);
	}

	
	workResponse response = workers[freeWorker]->requestWork(*request);
	if (response.status())
	{
		std::unique_lock<std::mutex> lock(nodeMutex);
		workerStatuses[freeWorker] = IDLE;
		availableWorkers++;
		nodeCV.notify_one();
		mapStatus[shardId] = 1;
		lock.unlock();

		std::unique_lock<std::mutex> lock2(intermediateFileMutex);
		for (size_t i = 0; i < response.outputfile_size(); i++)
		{
			intermediateFiles.push_back(response.outputfile(i));
		}
		
	}
	else
	{
		workerStatuses[freeWorker] = DOWN;
		mapNode(shardId);
	}
}

void Master::reduceNode(int reduceId){
	int freeWorker = getFreeWorker();
	workRequest *request;
	request->set_requesttype("REDUCE");
	request->set_userid(mrSpec.user_id);
	request->set_rpartition(mrSpec.n_workers);
	request->set_outputdir(mrSpec.output_dir);
	request->set_rpartition(mrSpec.n_output_files);
	for (size_t i = 0; i < intermediateFiles.size(); i++)
	{
		File *newFile = request->add_shard();
		newFile->set_filepath(intermediateFiles[i]);
	}

	workResponse response = workers[freeWorker]->requestWork(*request);
	if (response.status())
	{
		std::unique_lock<std::mutex> lock(nodeMutex);
		workerStatuses[freeWorker] = IDLE;
		availableWorkers++;
		nodeCV.notify_one();
		reduceStatus[reduceId] = 1;
		lock.unlock();
	}
	else
	{
		workerStatuses[freeWorker] = DOWN;
		reduceNode(reduceId);
	}
	
}

void Master::waitTillAllWorkersAreDone(){
	for (int i = 0; i < nodeThreads.size(); i++) {
		nodeThreads[i].join();
	}
	nodeThreads.clear();
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// initialize threadpool

	// send map requests to workers
	for (int i = 0; i < fileShards.size(); i++) {
		nodeThreads.push_back(std::move(std::thread(&Master::mapNode, this, i)));
	}

	// wait for all map requests to complete
	waitTillAllWorkersAreDone();

	// send reduce requests to workers
	for (int i = 0; i < mrSpec.n_output_files; i++) {
		nodeThreads.push_back(std::move(std::thread(&Master::reduceNode, this, i)));
	}

	waitTillAllWorkersAreDone();
	
	return true;
}