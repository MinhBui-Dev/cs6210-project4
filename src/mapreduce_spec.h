#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;
	std::vector<std::string> worker_ipaddr_ports;
	std::vector<std::string> input_files;
	std::string output_dir;
	int n_output_files;
	int map_kilobytes;
	std::string user_id;
};

// Snippet from Stack Overflow https://stackoverflow.com/a/55263720
inline std::vector<std::string> split_str(std::string str) {
	std::string tmp; 
	std::stringstream ss(str);
	std::vector<std::string> result;

	while (std::getline(ss, tmp, ',')){
		result.push_back(tmp);
	}
	return result;
}

inline void store_line(std::string key, std::string value, MapReduceSpec& mr_spec) {
	if ("n_workers" == key) {
		mr_spec.n_workers = stoi(value);
	} else if ("worker_ipaddr_ports" == key) {
		mr_spec.worker_ipaddr_ports = split_str(value);
	} else if ("input_files" == key) {
		mr_spec.input_files = split_str(value);
	} else if ("output_dir" == key) {
		mr_spec.output_dir = value;
	} else if ("n_output_files" == key) {
		mr_spec.n_output_files = stoi(value);
	} else if ("map_kilobytes" == key) {
		mr_spec.map_kilobytes = stoi(value);
	} else if ("user_id" == key) {
		mr_spec.user_id = value;
	}
	return;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	// Snippet from Stack Overflow https://stackoverflow.com/a/6892829
	std::ifstream is_file;
	is_file.open(config_filename, std::ifstream::in);
	std::string line;
	while (std::getline(is_file, line)) {
		std::istringstream is_line(line);
		std::string key;
		if (std::getline(is_line, key, '=')) {
			std::string value;
			if (std::getline(is_line, value)) 
				store_line(key, value, mr_spec);
		}
	}
	is_file.close();
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if (mr_spec.n_workers < 1) {
		return false;
	}
	if (mr_spec.worker_ipaddr_ports.size() != mr_spec.n_workers) {
		return false;
	}
	for (int i = 0; i < mr_spec.worker_ipaddr_ports.size(); i++) {
		if (mr_spec.worker_ipaddr_ports[i] == "") {
			return false;
		}
	}
	if (mr_spec.input_files.size() < 1) {
		return false;
	}
	for (int i = 0; i < mr_spec.input_files.size(); i++) {
		if (mr_spec.input_files[i] == "") {
			return false;
		}
	}
	if (mr_spec.output_dir == "") {
		return false;
	}
	if (mr_spec.n_output_files < 1) {
		return false;
	}
	if (mr_spec.map_kilobytes < 1) {
		return false;
	}
	if (mr_spec.user_id == "") {
		return false;
	}
	return true;
}
