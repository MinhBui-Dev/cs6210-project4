#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <set>
#include <vector>
#include <sys/stat.h>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal
{

	/* DON'T change this function's signature */
	BaseMapperInternal();

	/* DON'T change this function's signature */
	void emit(const std::string &key, const std::string &val);

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	std::string outputDir;
	std::string timestamp;
	std::map<std::string, std::map<std::string, std::string>> keyValMap;
	std::set<std::string> filesList; // to keep track of files created without duplicate name
	int rPartitions;
	int keycount;
	std::string getFileName(std::string key);
	void dumpToFile();
	std::vector<std::string> getListFiles();
};

/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal()
{
	keycount = 0;

	auto current = std::chrono::system_clock::now();
	std::time_t now = std::chrono::system_clock::to_time_t(current);
	std::stringstream ss;
	ss << std::put_time(std::localtime(&now), "%Y-%m-%d-%H-%M-%S");
	timestamp = ss.str();
}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string &key, const std::string &val)
{
	std::string fileName = getFileName(key);

	// Temporarily store the key value pair in the map
	if (keyValMap.find(fileName) == keyValMap.end())
	{
		std::map<std::string, std::string> temp;
		temp[key] = val;
		keyValMap[fileName] = temp;
		filesList.insert(fileName);

	}
	else
	{
		if (keyValMap[fileName].find(key) == keyValMap[fileName].end())
		{
			keyValMap[fileName][key] = val;
			keycount++;
		}
		else
		{
			int count = std::stoi(keyValMap[fileName][key]);
			keyValMap[fileName][key] = std::to_string(count + std::stoi(val));
			keycount++;
		}
	}

	// Dump the map to file if the number of keys exceeds 100
	if (keycount >= 100)
	{
		dumpToFile();
		keycount = 0;
	}
}

// Dump the keyValMap to file
inline void BaseMapperInternal::dumpToFile()
{
	for (auto it = keyValMap.begin(); it != keyValMap.end(); it++)
	{
		std::string fileName = it->first;
		std::map<std::string, std::string> temp = it->second;
		std::ofstream file;
		mkdir(outputDir.c_str(), 0777);
		file.open(fileName, std::ofstream::out | std::ofstream::app);
		for (auto it2 = temp.begin(); it2 != temp.end(); it2++)
		{
			file << it2->first << " " << it2->second << std::endl;
		}
		file.close();
	}
	keyValMap.clear();
}

// Partitions the key into rPartitions number of files using hash function
inline std::string BaseMapperInternal::getFileName(std::string key)
{
	std::string fileName = outputDir + "/" + std::to_string(std::hash<std::string>{}(key) % rPartitions) + "_" + timestamp + ".txt";
	return fileName;
}

// Returns the list of files created by the mapper
inline std::vector<std::string> BaseMapperInternal::getListFiles()
{
	std::vector<std::string> files;
	for (auto file: filesList)
	{
		files.push_back(file);
	}

	return files;
}

/*-----------------------------------------------------------------------------------------------*/

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal
{

	/* DON'T change this function's signature */
	BaseReducerInternal();

	/* DON'T change this function's signature */
	void emit(const std::string &key, const std::string &val);

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	std::string outputDir;
	std::string outputFile;
};

/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal()
{
}

/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string &key, const std::string &val)
{
	std::ofstream file;
	std::string fileName = outputDir + "/" + outputFile;
	file.open(fileName, std::ofstream::out | std::ofstream::app);
	file << key << " " << val << std::endl;
	file.close();
}
