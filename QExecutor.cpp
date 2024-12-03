#include <nlohmann/json.hpp>
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
#include <string>
#include <unordered_map>
#include <regex>
std::string getValueByKey(std::string key,std::map<std::string,std::string> &values)
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
	std::map<std::string, std::string> values;
	const std::regex expr(R"(([a-zA-Z]*)\(([^()]+)\))");
	const std::regex value_expr(R"((.*)=(.*))");
	const std::regex count_expr(R"((.*[^!<>])(=|!=|<=|>=|<|>)(.*))");
	const std::regex calc_expr(R"(\((.*)(\+|\-|\*|\/)(.*)\))");
	const std::regex easy_expr(R"(\(([0-9]*)(\+|\-|\*|\/)([0-9]*)\))");
	int world_size, world_rank;
	std::vector<char> Op;
	std::vector<std::string> output_names;
	std::filesystem::current_path(argv[1]);
	std::ifstream vals("values.txt"); // считали значения
	if (vals.is_open())
	{
		std::string line;
		std::getline(vals, line);
		size_t pos = 0;
		std::string token;
		std::string delimiter = ";";
		while ((pos = line.find(delimiter)) != std::string::npos) { //считываем обозначения
			token = line.substr(0, pos);
			output_names.push_back(token);
			line.erase(0, pos + delimiter.length());
		}
		while (std::getline(vals, line))
		{
			std::smatch Match;
			if (std::regex_search(line, Match, value_expr))
			{
				values[Match[1].str()] = Match[2].str(); //заполняем значения
			}
		}
	}
	std::errc errcode;
	std::vector<long long> result(output_names.size(), 0);
	std::vector<long long> l_result(output_names.size(), 0);
	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	double start = MPI_Wtime();
	std::vector<int> displs(world_size, 0);
	std::vector<int> sendcounts(world_size, 0);
	int sum = 0;
	int rem = output_names.size() % world_size;
	for (int i = 0; i < world_size; i++)
	{
		sendcounts[i] = output_names.size() / world_size;
		if (rem > 0) {
			sendcounts[i]++;
			rem--;
		}

		displs[i] = sum;
		sum += sendcounts[i];

	}
	MPI_Scatterv(result.data(), sendcounts.data(), displs.data(), MPI_LONG_LONG, l_result.data(), output_names.size(), MPI_LONG_LONG, 0, MPI_COMM_WORLD);
	unsigned long long answers_count = 0;
	unsigned long long dirNum = world_rank;
	char dirName[255];
	sprintf_s(dirName, "%llu", dirNum);
	while (std::filesystem::is_directory(dirName))
	{
		std::vector<std::string> filesInDir;
		//std::filesystem::current_path(dirName);
		for (const auto& entry : std::filesystem::directory_iterator(dirName)) //в каждой папке просматриваем файлы
		{
			filesInDir.push_back(entry.path().string());
		}
#pragma omp parallel for
		for (int i = 0; i < filesInDir.size();i++) //в каждой папке просматриваем файлы
		{
			std::string token;
			bool isAnswer = true;
			std::ifstream in(filesInDir[i]);
			if (in.is_open()) //открываем файл
			{
				std::string line;
				std::getline(in, line); //считываем значения
				size_t pos = 0;
				std::string delimiter = ";";
				while ((pos = line.find(delimiter)) != std::string::npos) { //и рассчитываем их значения
					std::smatch m;
					token = line.substr(0, pos);
					try
					{
						while (std::regex_search(token, m, expr))
						{
							token = std::regex_replace(token, expr, getValueByKey(m[0], values), std::regex_constants::format_first_only);
							while (std::regex_search(token, m, easy_expr))
							{
								std::string f = m[1].str();
								std::string op = m[2].str();
								std::string s = m[3].str();
								std::string x;
								if (op == "+") x = std::to_string(stoll(f) + stoll(s));
								if (op == "-") x = std::to_string(stoll(f) - stoll(s));
								if (op == "*") x = std::to_string(stoll(f) * stoll(s));
								if (op == "/") x = std::to_string(stoll(f) / stoll(s));
								token = std::regex_replace(token, easy_expr, x, std::regex_constants::format_first_only);
							}
						}
						line.erase(0, pos + delimiter.length());
					}
					catch (std::exception e)
					{
						int r = 0;
						std::cout << e.what() << std::endl;
					}
				}
				while (std::getline(in, line)) // дальше считываем операции
				{
					line = line.substr(1, line.size() - 2);
					std::smatch m;
					try
					{
						while (std::regex_search(line, m, expr))
						{
							line = std::regex_replace(line, expr, getValueByKey(m[0], values), std::regex_constants::format_first_only);

							while (std::regex_search(line, m, easy_expr))
							{
								std::string f = m[1].str();
								std::string op = m[2].str();
								std::string s = m[3].str();
								std::string x;
								if (op == "+") x = std::to_string(stoll(f) + stoll(s));
								if (op == "-") x = std::to_string(stoll(f) - stoll(s));
								if (op == "*") x = std::to_string(stoll(f) * stoll(s));
								if (op == "/") x = std::to_string(stoll(f) / stoll(s));
								line = std::regex_replace(line, easy_expr, x, std::regex_constants::format_first_only);
							}
						}
					}
					catch (std::exception e)
					{
						std::cout << e.what() << std::endl;

						std::cout << e.what() << std::endl;
					}
					std::smatch Match;
					if (std::regex_search(line, Match, count_expr))
					{
						std::string f = Match[1].str();
						std::string op = Match[2].str();
						std::string s = Match[3].str();

						if (!(stoll(f) == stoll(s) && op == "==" ||
							stoll(f) != stoll(s) && op == "!=" ||
							stoll(f) <= stoll(s) && op == "<=" ||
							stoll(f) >= stoll(s) && op == ">=" ||
							stoll(f) > stoll(s) && op == ">" ||
							stoll(f) < stoll(s) && op == "<"))
						{
							isAnswer = false;
							break;
						}
					}
				}
			#pragma omp critical
				if (isAnswer)
				{
					//result[dirNum] = token;
					answers_count++;
				}
			}
			in.close();
			if (isAnswer)
			{
				try
				{
					std::smatch Match;
					if (std::regex_search(token, Match, value_expr))
					{
						std::string f = Match[1].str();
						std::string op = Match[2].str();
						std::string s = Match[3].str();
						if (op == "+") token = std::to_string(stoll(values[f]) + stoll(values[s]));
						if (op == "-") token = std::to_string(stoll(values[f]) - stoll(values[s]));
						if (op == "*") token = std::to_string(stoll(values[f]) * stoll(values[s]));
						if (op == "/") token = std::to_string(stoll(values[f]) / stoll(values[s]));

					}
					l_result[dirNum % output_names.size()] = stoll(token);
				}
				catch (std::exception e)
				{
					std::cout << e.what() << std::endl;
				}
			}
		}
			dirNum += world_size;
			sprintf_s(dirName, "%llu", dirNum);
		}
		MPI_Gatherv(l_result.data(), l_result.size(), MPI_LONG_LONG, result.data(), sendcounts.data(), displs.data(), MPI_LONG_LONG, 0, MPI_COMM_WORLD);
		if (world_rank == 0)
		{
			std::ofstream out("answer.txt");
			if (out.is_open())
			{
				for (int i = 0; i < result.size(); i++)
					out << output_names[i] << "=" << result[i] << std::endl;

			}
			out.close();
		}
		double stop = MPI_Wtime();
		std::cout << stop - start << std::endl;
		MPI_Finalize();
		return 0;
	}
