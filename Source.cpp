#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include<string>
#include <sstream>
#include <cstdlib>
#include <filesystem>
#include <omp.h>
#include "mpi.h"
#include<regex>
std::map<std::string, std::string> values;
const std::regex expr(R"(([a-zA-Z]*)\(([^()]+)\))");
const std::regex value_expr(R"((.*)=(.*))");
std::string getValueByKey(std::string key)
{
	std::string str;
	if (values.find(key) != values.end())
		str = values[key];
	else
		str = key;
	return  str;
}
int main(int argc, char** argv)
{
	int world_size, world_rank;
	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	std::ifstream in(argv[1]);
	if (in.is_open())
	{
		std::string line;
		while (std::getline(in, line))
		{
			std::smatch Match;
			if (std::regex_search(line, Match, value_expr))
			{
				values[Match[1].str()] = Match[2].str();
			}
		}
	}
	unsigned long long num=0;
	char dir[255];
	while (true)
	{

		num += world_rank;
		_ui64toa_s(num, dir, sizeof(dir), 36);
		std::filesystem::current_path(dir);
		const std::filesystem::directory_iterator end{};

		for (std::filesystem::directory_iterator iter{ dir}; iter != end; ++iter)
		{
			unsigned long long index = 0;
			std::string file= iter->path().string();
			if (std::filesystem::is_regular_file(*iter))
			{
				if (file.find("I", index) != std::string::npos ||
					file.find("O", index) != std::string::npos)
					continue;
				else
				{
					std::ifstream in(file);
					if (in.is_open())
					{
						std::string line;
						while (std::getline(in, line))
						{
							std::regex_replace(line, expr, getValueByKey("$&"));

						}
					}
					in.close();
				}
			}
		}
		
	}
	MPI_Finalize();

}