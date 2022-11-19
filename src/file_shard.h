#pragma once

#include <vector>
#include <iostream>
#include <fstream>
#include <math.h>
#include "mapreduce_spec.h"


struct FileOffset {
    std::string filename;
    size_t start;
    size_t end;
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
    std::vector<FileOffset> file_offsets;
};

// Snippet from https://www.tutorialspoint.com/how-can-i-get-a-file-s-size-in-cplusplus
inline size_t get_file_size(std::string filepath) {
    std::ifstream in_file(filepath, std::ios::binary);
    in_file.seekg(0, std::ios::end);
    size_t file_size = in_file.tellg();
    in_file.close();
    return file_size;
}

inline size_t get_input_size(std::vector<std::string> filepaths) {
    size_t sum = 0;
    for (std::string filepath : filepaths) {
        sum += get_file_size(filepath);
    }
    return sum;
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
    int number_of_shards = ceil(
        (float)get_input_size(mr_spec.input_files) 
      / (float)(mr_spec.map_kilobytes * 1000)
    );
    int files_index = 0;
    size_t current_offset = 0;
    std::ifstream current_file(mr_spec.input_files[files_index], std::ios::in);
    std::string line;
    for (int i = 0; i < number_of_shards; i++) {
        size_t bytes_read = 0;
        size_t start_offset;
        
        // if new shard is at beginning of file, start at 0
        if (current_offset == 0) {
            start_offset = 0;
        } else { // if new shard is in middle of file, use next value after current_offset
            start_offset = current_offset + 1;
        }
        
        FileShard file_shard;
        while (bytes_read < mr_spec.map_kilobytes * 1000) {
            // read line by line
            std::getline(current_file, line);
            bytes_read += line.size() + 1;
            current_offset += bytes_read;

            if (current_file.eof()) {
                // create offset and add to shard
                FileOffset file_offset = {
                    mr_spec.input_files[files_index],
                    start_offset, 
                    // if last line there is no \n
                    current_offset - 1
                };
                file_shard.file_offsets.push_back(file_offset);

                // reset to next file
                current_file.clear();
                current_file.close();
                files_index++;
                current_offset = 0;
                start_offset = 0;
                current_file.open(mr_spec.input_files[files_index], std::ios::binary);
            }
        }
        // only create another FileOffset if valid file in input_files
        if (files_index < mr_spec.input_files.size()) {
            // create offset and add to shard
            FileOffset file_offset = {
                mr_spec.input_files[files_index], 
                start_offset,
                current_offset
            };
            file_shard.file_offsets.push_back(file_offset);
        }
        fileShards.push_back(file_shard);
    }
    return true;
}
