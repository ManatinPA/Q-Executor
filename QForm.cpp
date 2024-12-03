#include <libpq-fe.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <map>
#include <set>
#include<string>
#include <filesystem>
#include <omp.h>
#include <unordered_map>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <regex>
#define UNUSED(x) (void)(x)
std::regex calc_expr(R"(\((.*)(<[=>]?|==|!=|>=?|\+|\-|\/|\*)(.*)\))");
static PGconn* conn = NULL;
static PGresult* res = NULL;

static void terminate(int code)
{
	if (code != 0)
		fprintf(stderr, "%s\n", PQerrorMessage(conn));

	if (res != NULL)
		PQclear(res);

	if (conn != NULL)
		PQfinish(conn);

	exit(code);
}

static void clearRes(PGresult* res)
{
	PQclear(res);
	res = NULL;
}


int main2()
{
	int libpq_ver = PQlibVersion();
	printf("Version of libpq: %d\n", libpq_ver);
	conn = PQconnectdb("user=postgres password=postgres host=127.0.0.1 dbname=logs");
	if (PQstatus(conn) != CONNECTION_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	int server_ver = PQserverVersion(conn);
	char* user = PQuser(conn);
	char* db_name = PQdb(conn);
	printf("Server version: %d\n", server_ver);
	printf("User: %s\n", user);
	printf("Database name: %s\n", db_name);
	res = PQexec(conn, "CREATE TABLE IF NOT EXISTS phonebook "
		"(id SERIAL PRIMARY KEY, name VARCHAR(64), "
		"phone VARCHAR(64), last_changed TIMESTAMP)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "DELETE FROM phonebook");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	const char* query =
		"INSERT INTO phonebook (name, phone, last_changed) "
		" VALUES ($1, $2, now());";
	const char* params[2];
	res = PQexec(conn, "SELECT id, name, phone, last_changed "
		"FROM phonebook");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	int ncols = PQnfields(res);
	printf("There are %d columns:", ncols);
	for (int i = 0; i < ncols; i++)
	{
		char* name = PQfname(res, i);
		printf(" %s", name);
	}
	printf("\n");
	int nrows = PQntuples(res);
	for (int i = 0; i < nrows; i++)
	{
		char* id = PQgetvalue(res, i, 0);
		char* name = PQgetvalue(res, i, 1);
		char* phone = PQgetvalue(res, i, 2);
		char* last_changed = PQgetvalue(res, i, 3);
		printf("Id: %s, Name: %s, Phone: %s, Last changed: %s\n",
			id, name, phone, last_changed);
	}
	printf("Total: %d rows\n", nrows);
	clearRes(res);
	terminate(0);
	return 0;
}
class QTerm
{
public:
	std::string string;
	std::string op;
	std::unique_ptr<QTerm> fO;
	std::unique_ptr<QTerm> sO;
	QTerm* ancestor;
	std::unordered_map < std::string, unsigned long long> operationsDict;
	std::set<std::string> inputData;
	QTerm() {}
	QTerm(std::string str, QTerm* ancestor = nullptr, bool final = false)
	{
		if (final)
		{
			string = str;
		}
		else
		{
			std::string fo, so;
			nlohmann::json jstr = nlohmann::json::parse(str, nullptr, false);
#pragma omp parallel sections
			{
#pragma omp section
				{
					fo = to_string(jstr["fO"]);
				}
#pragma omp section
				{
					so = to_string(jstr["sO"]);
				}
			}
			if (!fo.empty() && !so.empty())
			{
#pragma omp parallel sections
				{
#pragma omp section
					{
						std::replace(fo.begin(), fo.end(), '[', '(');
						std::replace(fo.begin(), fo.end(), ']', ')');
					}
#pragma omp section
					{
						std::replace(so.begin(), so.end(), '[', '(');
						std::replace(so.begin(), so.end(), ']', ')');
					}
#pragma omp section
					{
						if (jstr["op"].is_string())
						{
							op = to_string(jstr["op"]);
							op.erase(std::remove(op.begin(), op.end(), '\"'), op.end());
						}
					}
				}
#pragma omp parallel sections
				{
#pragma omp section
					{
						if (jstr["fO"].is_object())
							fO = std::make_unique<QTerm>(fo, this);
						else
						{
							this->ancestor = ancestor;
							fo.erase(std::remove(fo.begin(), fo.end(), '"'), fo.end());
							if (jstr["sO"].is_object())
							{

								so = get_expression(jstr["sO"]).c_str();
								so.erase(std::remove(so.begin(), so.end(), '"'), so.end());
							}
							if (!isNumeric(fo))
							{
								inputData.insert(fo);
								const char* query =
									"INSERT INTO qdeterminant_vars_and_values(var) "
									" VALUES ($1) on conflict do nothing;";
								const char* params[3];
								params[0] = fo.data();
								res = PQexecParams(conn, query, 1, NULL, params,
									NULL, NULL, 0);
								if (PQresultStatus(res) != PGRES_COMMAND_OK)
									terminate(1);
								clearRes(res);
								{
									so.erase(std::remove(so.begin(), so.end(), '"'), so.end());
									query =
										"INSERT INTO qterms_logical(fo,op,so) "
										" VALUES ($1,$2,$3) on conflict do nothing;";
									params[0] = fo.data();
									params[1] = op.data();
									params[2] = so.data();

									res = PQexecParams(conn, query, 3, NULL, params,
										NULL, NULL, 0);
									if (PQresultStatus(res) != PGRES_COMMAND_OK)
										terminate(1);
									clearRes(res);
								}
							}
						}
					}
#pragma omp section
					{
						if (jstr["sO"].is_object())
						{
							sO = std::make_unique<QTerm>(so, this);

						}
						else
						{
							this->ancestor = ancestor;
							so.erase(std::remove(so.begin(), so.end(), '"'), so.end());
							if (jstr["fO"].is_object())
							{
								fo = get_expression(jstr["fO"]).c_str();
								fo.erase(std::remove(fo.begin(), fo.end(), '"'), fo.end());
							}
							so.erase(std::remove(so.begin(), so.end(), '"'), so.end());
							if (!isNumeric(so))
							{
								inputData.insert(so);
								const char* query =
									"INSERT INTO qdeterminant_vars_and_values(var) "
									" VALUES ($1) on conflict do nothing;";
								const char* params[3];
								params[0] = so.data();
								res = PQexecParams(conn, query, 1, NULL, params,
									NULL, NULL, 0);
								if (PQresultStatus(res) != PGRES_COMMAND_OK)
									terminate(1);
								clearRes(res);
								{
									so.erase(std::remove(so.begin(), so.end(), '"'), so.end());
									query =
										"INSERT INTO qterms_logical(fo,op,so) "
										" VALUES ($1,$2,$3) on conflict do nothing;";
									params[0] = fo.data();
									params[1] = op.data();
									params[2] = so.data();

									res = PQexecParams(conn, query, 3, NULL, params,
										NULL, NULL, 0);
									if (PQresultStatus(res) != PGRES_COMMAND_OK)
										terminate(1);
									clearRes(res);
								}
							}
						}
					}
				}
			}
#pragma omp section
			{

				string = get_expression(jstr);
				if (fO == nullptr && sO == nullptr && !fo.empty() && !so.empty())
				{
					std::smatch Match;
					if (std::regex_search(string, Match, calc_expr))
					{
						fO = std::make_unique<QTerm>(Match[1].str(), this, true);
						op = Match[2].str();
						sO = std::make_unique<QTerm>(Match[3].str(), this, true);
					}

				}
			}
			if (ancestor != nullptr)
			{
#pragma omp parallel sections
				{
#pragma omp section
					{
						for (std::unordered_map<std::string, unsigned long long>::iterator it = operationsDict.begin(); it != operationsDict.end(); ++it)
							ancestor->operationsDict[it->first]++;
					}
#pragma omp section
					{
						for (std::set<std::string>::iterator it = inputData.begin(); it != inputData.end(); ++it)
							ancestor->inputData.insert(*it);
					}
				}
				if (string != "" && stringIsCorrect(string))
					ancestor->operationsDict[string]++;
			}
			else
			{
				if (string != "" && stringIsCorrect(string))
					operationsDict[string]++;
			}
		}
	}
private:
	static bool isNumeric(std::string const& str)
	{
		return !str.empty() && std::all_of(str.begin(), str.end(), ::isdigit);
	}
	static unsigned long long countOfSubstr(std::string str, std::string substr)
	{
		unsigned long long index = 0;
		unsigned long long count = 0;
		while ((index = str.find(substr, index)) != std::string::npos) {
			index += substr.length();
			count++;
		}
		return count;
	}
	static bool stringIsCorrect(std::string str)
	{
		unsigned long long count = 0;
		const std::vector<std::string> charList = { "&","<",">","!=","==" };
#pragma omp parallel for reduction(+:count)
		for (long long i = 0; i < charList.size(); i++)
		{
			count += countOfSubstr(str, charList[i]);
		}
		if (count == 1) return true;
		return false;
	}
	static std::string get_expression(nlohmann::json jstr)
	{
		if (!jstr.is_string())
			return "(" + get_expression(jstr["fO"]) + get_expression(jstr["op"]) + get_expression(jstr["sO"]) + ")";
		std::string text = to_string(jstr);
		text.erase(std::remove(text.begin(), text.end(), '"'), text.end());
		return text;
	}
};
class QString
{

public:
	QTerm logical = QTerm();
	std::vector<std::string> values;
	std::map<std::string, unsigned long long> data;
	std::unordered_map<std::string, unsigned long long> operations;
	QString(std::string line, std::unordered_map<std::string, std::set<QString*>>& Operations, std::unordered_map<std::string, std::set<QString*>>& Data)
	{
		std::string element;
		std::stringstream ss(line.substr(line.find(';') + 1));
		std::string segment;
#pragma omp parallel sections
		{
#pragma omp section
			{
				std::string element = line.substr(0, line.find('='));
				data[element]++;
				std::string logical_part = line.substr(0, line.find(";", 0)).substr(line.find('=') + 1);
				logical_part.erase(std::remove(logical_part.begin(), logical_part.end(), ' '));
				if (logical_part != "")
					logical = QTerm(line.substr(0, line.find(";", 0)).substr(line.find('=') + 1));
			}
#pragma omp section
			{
				while (std::getline(ss, segment, ';'))
				{
					nlohmann::json x = nlohmann::json::parse(segment, nullptr, false);
					if (x.is_string() || x.is_discarded())
					{
						values.push_back(segment);
						//data[segment]++;
					}
					else if (x.is_number())
					{
						values.push_back(segment);
					}
					else if (x.is_object())
					{
						auto y = QTerm(segment);
						values.push_back(y.string);
					}
				}
			}
		}
#pragma omp parallel sections
		{
#pragma omp section
			{
				for (auto it = logical.operationsDict.begin(); it != logical.operationsDict.end(); ++it)
					operations[it->first]++;
			}
		}
		fillTheLists(Operations, Data);
	}
private:
	void fillTheLists(std::unordered_map<std::string, std::set<QString*>>& Operations, std::unordered_map<std::string, std::set<QString*>>& Data)
	{
#pragma omp parallel sections
		{
#pragma omp section
			{
				//#pragma omp parallel for
				for (auto it = data.begin(); it != data.end(); ++it)
					Data[it->first].insert(this);
			}
#pragma omp section
			{
				//#pragma omp parallel for
				for (auto it = operations.begin(); it != operations.end(); ++it)
					Operations[it->first].insert(this);
			}
		}
	}
};
int main(int argc, char** argv)
{
	double start = omp_get_wtime();
	std::unordered_map<std::string, std::set<QString*>> operations;
	std::unordered_map<std::string, std::set<QString*>> data;
	std::unordered_map <std::string, std::string> files;
	std::set<QString*> listofQStrings;
	std::ifstream in(argv[1]);
	int libpq_ver = PQlibVersion();
	printf("Version of libpq: %d\n", libpq_ver);
	conn = PQconnectdb("user=postgres password=postgres host=127.0.0.1 dbname=logs");
	if (PQstatus(conn) != CONNECTION_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	int server_ver = PQserverVersion(conn);
	char* user = PQuser(conn);
	char* db_name = PQdb(conn);
	printf("Server version: %d\n", server_ver);
	printf("User: %s\n", user);
	printf("Database name: %s\n", db_name);
	res = PQexec(conn, "DROP Table if exists qdeterminant");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "DROP Table if exists qterms_logical");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "DROP Table if exists qterms_arithm");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "CREATE TABLE IF NOT EXISTS qdeterminant"
		"(id SERIAL PRIMARY KEY, var VARCHAR(64),"
		"qterm VARCHAR(64), result VARCHAR(64))");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "CREATE TABLE IF NOT EXISTS qterms_logical "
		"(id SERIAL PRIMARY KEY, op VARCHAR(64),"
		"fO VARCHAR(64), fO_type VARCHAR(64), sO VARCHAR(64), sO_type VARCHAR(64), UNIQUE  (fO,op,sO))");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "DROP Table if exists qdeterminant_vars_and_values");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "CREATE TABLE IF NOT EXISTS qterms_arithm "
		"(id SERIAL PRIMARY KEY, op VARCHAR(64),"
		"fO VARCHAR(64), fO_type VARCHAR(64), sO VARCHAR(64), sO_type VARCHAR(64),"
		"UNIQUE  (fO,op,sO))");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	res = PQexec(conn, "CREATE TABLE IF NOT EXISTS qdeterminant_vars_and_values "
		"(id SERIAL PRIMARY KEY, var VARCHAR(64),"
		"value VARCHAR(64),"
		"CONSTRAINT var_uniq UNIQUE (var))");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	clearRes(res);
	if (in.is_open())
	{
		std::string line;
		unsigned long long dirNum = 0;
		unsigned long long fileNum = 0;
		while (std::getline(in, line))
		{
			auto it = QString(line, operations, data);
			std::error_code code;
			auto element = &it;
			std::string var;
			for (auto& x : element->data)
			{
				var += x.first;
			}
			std::string res2;
			for (auto& x : element->values)
			{
				res2 += x;
			}
			const char* query =
				"INSERT INTO qdeterminant(var, qterm, result) "
				" VALUES ($1, $2, $3);";
			const char* params[3];

			for (auto& y : element->operations)
			{
				std::string qterm = y.first;
				if (qterm.empty() || res2.empty())
					break;
				params[0] = var.data();
				params[1] = qterm.data();
				params[2] = res2.data();
				res = PQexecParams(conn, query, 3, NULL, params,
					NULL, NULL, 0);
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
					terminate(1);
				clearRes(res);
			}
		}

	}
	res = PQexec(conn, "SELECT op, fO, sO "
		"FROM qterms_logical Where op in ('+','-','/','*')");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		std::cout << "error";
		terminate(1);
	}

	int nrows = PQntuples(res);
	for (int i = 0; i < nrows; i++)
	{
		const char* query =
			"INSERT INTO qterms_arithm(op, fO, sO) "
			" VALUES ($1, $2, $3);";
		const char* params[3];
		params[0] = PQgetvalue(res, i, 0);
		params[1] = PQgetvalue(res, i, 1);
		params[2] = PQgetvalue(res, i, 2);
		std::string line = params[0];
		PGresult* res2 = PQexecParams(conn, query, 3, NULL, params,
			NULL, NULL, 0);
		if (PQresultStatus(res2) != PGRES_COMMAND_OK)
			terminate(1);
		clearRes(res2);
		query = "DELETE FROM qterms_logical where op=$1 and fO=$2 and sO=$3 ";
		res2 = PQexecParams(conn, query, 3, NULL, params,
			NULL, NULL, 0);
		if (PQresultStatus(res2) != PGRES_COMMAND_OK)
			terminate(1);
		clearRes(res2);
	}
	res = PQexec(conn, "SELECT id, op, fO, sO "
		"FROM qterms_arithm");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		std::cout << "error";
		terminate(1);
	}
	nrows = PQntuples(res);
	for (int i = 0; i < nrows; i++)
	{

		const char* query =
			"UPDATE qterms_logical SET fO = REPLACE(fO, CONCAT($3::text,$2::text,$4::text) , $1)";
		const char* params[4];



		params[0] = PQgetvalue(res, i, 0);
		params[1] = PQgetvalue(res, i, 1);
		params[2] = PQgetvalue(res, i, 2);
		params[3] = PQgetvalue(res, i, 3);

		PGresult* res2 = PQexecParams(conn, query, 4, NULL, params,
			NULL, NULL, 0);
		if (PQresultStatus(res2) != PGRES_COMMAND_OK)
			terminate(1);
		clearRes(res2);

		query =
			"UPDATE qterms_logical SET sO = REPLACE(sO, CONCAT($3::text,$2::text,$4::text), $1)";
		res2 = PQexecParams(conn, query, 4, NULL, params,
			NULL, NULL, 0);
		if (PQresultStatus(res2) != PGRES_COMMAND_OK)
			terminate(1);
		clearRes(res2);
	}
	terminate(0);
	return 0;
}
