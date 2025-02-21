#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <variant>
#include <memory>
#include <stdexcept>
#include <string>
#include <algorithm>

enum class ColumnType { INT, STRING };

struct Column {
    std::string name;
    ColumnType type;
};

using Row = std::vector<std::variant<int, std::string>>;

class Table {
private:
    std::vector<Column> schema;
    std::unordered_map<int, Row> data;
    std::shared_ptr<std::unordered_map<std::string, std::unordered_set<int>>> stringIndex;
    size_t indexedColumn = 0;

public:
    Table(const std::vector<Column>& s) : schema(s) {
        if (schema.empty() || schema[0].type != ColumnType::INT) {
            throw std::runtime_error("First column must be INT for primary key");
        }
    }

    const std::vector<Column>& getSchema() const { return schema; }

    void insert(const Row& row) {
        validateRow(row);
        int id = std::get<int>(row[0]);
        if (data.find(id) != data.end()) {
            throw std::runtime_error("Duplicate ID: " + std::to_string(id) + " already exists");
        }
        data[id] = row;
        updateIndex(id, row);
    }

    Row get(int id) const {
        auto it = data.find(id);
        if (it != data.end()) return it->second;
        throw std::runtime_error("ID not found");
    }

    void update(int id, const Row& newRow) {
        if (data.find(id) == data.end()) throw std::runtime_error("ID not found");
        validateRow(newRow);
        if (std::get<int>(newRow[0]) != id) throw std::runtime_error("Cannot change primary key");
        removeFromIndex(id);
        data[id] = newRow;
        updateIndex(id, newRow);
    }

    void remove(int id) {
        if (data.erase(id)) removeFromIndex(id);
    }

    void createIndex(size_t columnIndex) {
        if (columnIndex >= schema.size() || columnIndex == 0 || schema[columnIndex].type != ColumnType::STRING) {
            throw std::runtime_error("Invalid column for indexing");
        }
        stringIndex = std::make_shared<std::unordered_map<std::string, std::unordered_set<int>>>();
        indexedColumn = columnIndex;
        for (const auto& pair : data) {
            const std::string& value = std::get<std::string>(pair.second[columnIndex]);
            (*stringIndex)[value].insert(pair.first);
        }
    }

    std::vector<Row> selectByIndex(const std::string& value) const {
        std::vector<Row> results;
        if (stringIndex && indexedColumn > 0) {
            auto it = stringIndex->find(value);
            if (it != stringIndex->end()) {
                for (int id : it->second) results.push_back(data.at(id));
            }
        }
        return results;
    }

    void save(const std::string& filename) const {
        std::ofstream ofs(filename, std::ios::binary);
        if (!ofs) throw std::runtime_error("Cannot open file for writing");
        size_t schemaSize = schema.size();
        ofs.write(reinterpret_cast<const char*>(&schemaSize), sizeof(schemaSize));
        for (const auto& col : schema) {
            int nameLen = static_cast<int>(col.name.size());
            ofs.write(reinterpret_cast<const char*>(&nameLen), sizeof(nameLen));
            ofs.write(col.name.c_str(), nameLen);
            ofs.write(reinterpret_cast<const char*>(&col.type), sizeof(col.type));
        }
        size_t dataSize = data.size();
        ofs.write(reinterpret_cast<const char*>(&dataSize), sizeof(dataSize));
        for (const auto& pair : data) {
            int id = pair.first;
            ofs.write(reinterpret_cast<const char*>(&id), sizeof(id));
            for (const auto& field : pair.second) {
                if (std::holds_alternative<int>(field)) {
                    int val = std::get<int>(field);
                    ofs.write(reinterpret_cast<const char*>(&val), sizeof(val));
                } else {
                    const std::string& val = std::get<std::string>(field);
                    int len = static_cast<int>(val.size());
                    ofs.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    ofs.write(val.c_str(), len);
                }
            }
        }
    }

    void load(const std::string& filename) {
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs) return;
        data.clear();
        if (stringIndex) stringIndex->clear();
        size_t schemaSize;
        ifs.read(reinterpret_cast<char*>(&schemaSize), sizeof(schemaSize));
        if (schemaSize != schema.size()) throw std::runtime_error("Schema mismatch");
        for (size_t i = 0; i < schemaSize; ++i) {
            int nameLen;
            ifs.read(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));
            std::string name(nameLen, '\0');
            ifs.read(&name[0], nameLen);
            ColumnType type;
            ifs.read(reinterpret_cast<char*>(&type), sizeof(type));
            if (name != schema[i].name || type != schema[i].type) throw std::runtime_error("Schema mismatch");
        }
        size_t dataSize;
        ifs.read(reinterpret_cast<char*>(&dataSize), sizeof(dataSize));
        for (size_t i = 0; i < dataSize; ++i) {
            int id;
            ifs.read(reinterpret_cast<char*>(&id), sizeof(id));
            Row row;
            for (const auto& col : schema) {
                if (col.type == ColumnType::INT) {
                    int val;
                    ifs.read(reinterpret_cast<char*>(&val), sizeof(val));
                    row.push_back(val);
                } else {
                    int len;
                    ifs.read(reinterpret_cast<char*>(&len), sizeof(len));
                    std::string val(len, '\0');
                    ifs.read(&val[0], len);
                    row.push_back(val);
                }
            }
            data[id] = row;
            updateIndex(id, row);
        }
    }

private:
    void validateRow(const Row& row) const {
        if (row.size() != schema.size()) throw std::runtime_error("Row size does not match schema");
        for (size_t i = 0; i < schema.size(); ++i) {
            if ((schema[i].type == ColumnType::INT && !std::holds_alternative<int>(row[i])) ||
                (schema[i].type == ColumnType::STRING && !std::holds_alternative<std::string>(row[i]))) {
                throw std::runtime_error("Type mismatch in row");
            }
        }
    }

    void updateIndex(int id, const Row& row) {
        if (stringIndex && indexedColumn > 0) {
            const std::string& value = std::get<std::string>(row[indexedColumn]);
            (*stringIndex)[value].insert(id);
        }
    }

    void removeFromIndex(int id) {
        if (stringIndex && indexedColumn > 0) {
            auto rowIt = data.find(id);
            if (rowIt != data.end()) {
                const std::string& value = std::get<std::string>(rowIt->second[indexedColumn]);
                (*stringIndex)[value].erase(id);
                if ((*stringIndex)[value].empty()) stringIndex->erase(value);
            }
        }
    }
};

class Database {
private:
    std::unordered_map<std::string, std::unique_ptr<Table>> databases;
    Table* currentDb = nullptr;
    std::string currentDbName;

    enum class TokenType { KEYWORD, INT, STRING, PUNCTUATION, IDENTIFIER };

    struct Token {
        TokenType type;
        std::string value;
    };

    std::vector<Token> tokenize(const std::string& input) {
        std::vector<Token> tokens;
        std::string token;
        bool inString = false;

        for (size_t i = 0; i < input.size(); ++i) {
            char c = input[i];
            if (c == '\'') {
                if (inString) {
                    tokens.push_back({TokenType::STRING, token});
                    token.clear();
                    inString = false;
                } else {
                    inString = true;
                }
            } else if (inString) {
                token += c;
            } else if (std::isspace(c)) {
                if (!token.empty()) {
                    addToken(tokens, token);
                    token.clear();
                }
            } else if (c == '(' || c == ')' || c == ',' || c == '=') {
                if (!token.empty()) {
                    addToken(tokens, token);
                    token.clear();
                }
                tokens.push_back({TokenType::PUNCTUATION, std::string(1, c)});
            } else {
                token += c;
            }
        }
        if (!token.empty()) addToken(tokens, token);
        return tokens;
    }

    void addToken(std::vector<Token>& tokens, const std::string& token) {
        if (token == "CREATE" || token == "DATABASE" || token == "USE" || token == "SHOW" || token == "DATABASES" ||
            token == "DROP" || token == "INSERT" || token == "INTO" || token == "VALUES" ||
            token == "SELECT" || token == "FROM" || token == "WHERE" || token == "UPDATE" || token == "SET" || token == "DELETE") {
            tokens.push_back({TokenType::KEYWORD, token});
        } else if (std::all_of(token.begin(), token.end(), ::isdigit)) {
            tokens.push_back({TokenType::INT, token});
        } else {
            tokens.push_back({TokenType::IDENTIFIER, token});
        }
    }

    void parseCreate(const std::vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "CREATE", TokenType::KEYWORD);
        expect(tokens, i++, "DATABASE", TokenType::KEYWORD);
        if (i >= tokens.size() || tokens[i].type != TokenType::IDENTIFIER) {
            throw std::runtime_error("Expected database name after 'CREATE DATABASE'");
        }
        std::string dbName = tokens[i++].value;
        if (databases.find(dbName) != databases.end()) {
            throw std::runtime_error("Database '" + dbName + "' already exists");
        }
        databases[dbName] = std::make_unique<Table>(std::vector<Column>{{"id", ColumnType::INT}, {"name", ColumnType::STRING}});
        databases[dbName]->load(dbName + ".dat"); // Load if exists
        databases[dbName]->createIndex(1);
        currentDb = databases[dbName].get();
        currentDbName = dbName;
        std::cout << "Database '" << dbName << "' created and selected" << std::endl << std::flush;
    }

    void parseUse(const std::vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "USE", TokenType::KEYWORD);
        if (i >= tokens.size() || tokens[i].type != TokenType::IDENTIFIER) {
            throw std::runtime_error("Expected database name after 'USE'");
        }
        std::string dbName = tokens[i++].value;
        if (databases.find(dbName) == databases.end()) {
            throw std::runtime_error("Database '" + dbName + "' does not exist");
        }
        currentDb = databases[dbName].get();
        currentDbName = dbName;
        std::cout << "Switched to database '" << dbName << "'" << std::endl << std::flush;
    }

    void parseShowDatabases(const std::vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "SHOW", TokenType::KEYWORD);
        expect(tokens, i++, "DATABASES", TokenType::KEYWORD);
        if (i != tokens.size()) throw std::runtime_error("Extra tokens after 'SHOW DATABASES'");
        std::cout << "Databases:\n";
        for (const auto& pair : databases) {
            std::cout << "  " << pair.first << (pair.first == currentDbName ? " (current)" : "") << std::endl;
        }
        std::cout << std::flush;
    }

    void parseDropDatabase(const std::vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "DROP", TokenType::KEYWORD);
        expect(tokens, i++, "DATABASE", TokenType::KEYWORD);
        if (i >= tokens.size() || tokens[i].type != TokenType::IDENTIFIER) {
            throw std::runtime_error("Expected database name after 'DROP DATABASE'");
        }
        std::string dbName = tokens[i++].value;
        if (databases.find(dbName) == databases.end()) {
            throw std::runtime_error("Database '" + dbName + "' does not exist");
        }
        databases.erase(dbName);
        if (dbName == currentDbName) {
            currentDb = nullptr;
            currentDbName.clear();
            std::cout << "Database '" << dbName << "' dropped. No database selected." << std::endl << std::flush;
        } else {
            std::cout << "Database '" << dbName << "' dropped" << std::endl << std::flush;
        }
        // Optionally, delete the file (commented out to preserve data unless explicitly desired)
        // std::remove((dbName + ".dat").c_str());
    }

    void parseInsert(const std::vector<Token>& tokens) {
        if (!currentDb) throw std::runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
        size_t i = 0;
        expect(tokens, i++, "INSERT", TokenType::KEYWORD);
        expect(tokens, i++, "INTO", TokenType::KEYWORD);
        expect(tokens, i++, "table", TokenType::IDENTIFIER);
        expect(tokens, i++, "VALUES", TokenType::KEYWORD);
        expect(tokens, i++, "(", TokenType::PUNCTUATION);

        Row row;
        const auto& schema = currentDb->getSchema();
        for (size_t col = 0; col < schema.size(); ++col) {
            if (col > 0) expect(tokens, i++, ",", TokenType::PUNCTUATION);
            if (schema[col].type == ColumnType::INT) {
                expect(tokens, i++, TokenType::INT);
                row.push_back(std::stoi(tokens[i-1].value));
            } else {
                expect(tokens, i++, TokenType::STRING);
                row.push_back(tokens[i-1].value);
            }
        }
        expect(tokens, i++, ")", TokenType::PUNCTUATION);
        currentDb->insert(row);
        std::cout << "Inserted successfully into '" << currentDbName << "'" << std::endl << std::flush;
    }

    void parseSelect(const std::vector<Token>& tokens) {
        if (!currentDb) throw std::runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
        size_t i = 0;
        expect(tokens, i++, "SELECT", TokenType::KEYWORD);
        expect(tokens, i++, "*", TokenType::IDENTIFIER);
        expect(tokens, i++, "FROM", TokenType::KEYWORD);
        expect(tokens, i++, "table", TokenType::IDENTIFIER);
        expect(tokens, i++, "WHERE", TokenType::KEYWORD);
        expect(tokens, i++, "id", TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        if (i >= tokens.size() || tokens[i].type != TokenType::INT) {
            throw std::runtime_error("Expected integer value after 'id =' (e.g., 1)");
        }
        expect(tokens, i++, TokenType::INT);
        int id = std::stoi(tokens[i-1].value);
        if (i != tokens.size()) throw std::runtime_error("Extra tokens after command");
        Row row = currentDb->get(id);
        for (size_t j = 0; j < row.size(); ++j) {
            std::visit([](const auto& v) { std::cout << v; }, row[j]);
            if (j < row.size() - 1) std::cout << ", ";
        }
        std::cout << std::endl << std::flush;
    }

    void parseUpdate(const std::vector<Token>& tokens) {
        if (!currentDb) throw std::runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
        size_t i = 0;
        expect(tokens, i++, "UPDATE", TokenType::KEYWORD);
        expect(tokens, i++, "table", TokenType::IDENTIFIER);
        expect(tokens, i++, "SET", TokenType::KEYWORD);
        expect(tokens, i++, currentDb->getSchema()[1].name, TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        expect(tokens, i++, TokenType::STRING);
        std::string newValue = tokens[i-1].value;
        expect(tokens, i++, "WHERE", TokenType::KEYWORD);
        expect(tokens, i++, "id", TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        expect(tokens, i++, TokenType::INT);
        int id = std::stoi(tokens[i-1].value);
        Row row = currentDb->get(id);
        row[1] = newValue;
        currentDb->update(id, row);
        std::cout << "Updated successfully in '" << currentDbName << "'" << std::endl << std::flush;
    }

    void parseDelete(const std::vector<Token>& tokens) {
        if (!currentDb) throw std::runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
        size_t i = 0;
        expect(tokens, i++, "DELETE", TokenType::KEYWORD);
        expect(tokens, i++, "FROM", TokenType::KEYWORD);
        expect(tokens, i++, "table", TokenType::IDENTIFIER);
        expect(tokens, i++, "WHERE", TokenType::KEYWORD);
        expect(tokens, i++, "id", TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        expect(tokens, i++, TokenType::INT);
        int id = std::stoi(tokens[i-1].value);
        currentDb->remove(id);
        std::cout << "Deleted successfully from '" << currentDbName << "'" << std::endl << std::flush;
    }

    void expect(const std::vector<Token>& tokens, size_t i, const std::string& value, TokenType type) {
        if (i >= tokens.size() || tokens[i].type != type || tokens[i].value != value) {
            throw std::runtime_error("Syntax error at token " + std::to_string(i));
        }
    }

    void expect(const std::vector<Token>& tokens, size_t i, TokenType type) {
        if (i >= tokens.size() || tokens[i].type != type) {
            throw std::runtime_error("Syntax error at token " + std::to_string(i));
        }
    }

    void execute(const std::string& command) {
        auto tokens = tokenize(command);
        if (tokens.empty()) return;
        if (tokens[0].value == "CREATE") parseCreate(tokens);
        else if (tokens[0].value == "USE") parseUse(tokens);
        else if (tokens[0].value == "SHOW") parseShowDatabases(tokens);
        else if (tokens[0].value == "DROP") parseDropDatabase(tokens);
        else if (tokens[0].value == "INSERT") parseInsert(tokens);
        else if (tokens[0].value == "SELECT") parseSelect(tokens);
        else if (tokens[0].value == "UPDATE") parseUpdate(tokens);
        else if (tokens[0].value == "DELETE") parseDelete(tokens);
        else throw std::runtime_error("Unknown command: " + tokens[0].value);
    }

public:
    Database() = default;

    ~Database() {
        for (auto& pair : databases) {
            pair.second->save(pair.first + ".dat");
        }
    }

    void run() {
        std::cout << "Custom Database Engine\nCommands:\n"
                  << "  CREATE DATABASE dbname\n"
                  << "  USE dbname\n"
                  << "  SHOW DATABASES\n"
                  << "  DROP DATABASE dbname\n"
                  << "  INSERT INTO table VALUES (1, 'name')\n"
                  << "  SELECT * FROM table WHERE id = 1\n"
                  << "  UPDATE table SET name = 'newname' WHERE id = 1\n"
                  << "  DELETE FROM table WHERE id = 1\n"
                  << "  EXIT to quit\n";
        std::string line;
        while (true) {
            std::cout << (currentDbName.empty() ? "No DB> " : currentDbName + "> ") << std::flush;
            if (!std::getline(std::cin, line)) break; // Handle Ctrl+D or EOF gracefully
            if (line == "EXIT") break;
            try {
                execute(line);
            } catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << std::endl;
            }
        }
    }
};

int main() {
    try {
        Database db;
        db.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
