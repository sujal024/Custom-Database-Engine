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

using namespace std;

enum class ColumnType { INT, STRING };

struct Column {
    string name;
    ColumnType type;
};

using Row = vector<variant<int, string>>;

class Table {
private:
    vector<Column> schema;
    unordered_map<int, Row> data;
    shared_ptr<unordered_map<string, unordered_set<int>>> stringIndex;
    size_t indexedColumn = 0;

public:
    Table(const vector<Column>& s) : schema(s) {
        if (schema.empty() || schema[0].type != ColumnType::INT) {
            throw runtime_error("First column must be INT for primary key");
        }
    }

    const vector<Column>& getSchema() const { return schema; }

    void insert(const Row& row) {
        validateRow(row);
        int id = std::get<int>(row[0]);
        if (data.find(id) != data.end()) {
            throw runtime_error("Duplicate ID: " + to_string(id) + " already exists");
        }
        data[id] = row;
        updateIndex(id, row);
    }

    Row get(int id) const {
        auto it = data.find(id);
        if (it != data.end()) return it->second;
        throw runtime_error("ID not found");
    }

    void update(int id, const Row& newRow) {
        if (data.find(id) == data.end()) throw runtime_error("ID not found");
        validateRow(newRow);
        if (std::get<int>(newRow[0]) != id) throw runtime_error("Cannot change primary key");
        removeFromIndex(id);
        data[id] = newRow;
        updateIndex(id, newRow);
    }

    void remove(int id) {
        if (data.erase(id)) removeFromIndex(id);
    }

    void createIndex(size_t columnIndex) {
        if (columnIndex >= schema.size() || columnIndex == 0 || schema[columnIndex].type != ColumnType::STRING) {
            throw runtime_error("Invalid column for indexing");
        }
        stringIndex = make_shared<unordered_map<string, unordered_set<int>>>();
        indexedColumn = columnIndex;
        for (const auto& pair : data) {
            const string& value = std::get<string>(pair.second[columnIndex]);
            (*stringIndex)[value].insert(pair.first);
        }
    }

    vector<Row> selectByIndex(const string& value) const {
        vector<Row> results;
        if (stringIndex && indexedColumn > 0) {
            auto it = stringIndex->find(value);
            if (it != stringIndex->end()) {
                for (int id : it->second) results.push_back(data.at(id));
            }
        }
        return results;
    }

    void save(const string& filename) const {
        ofstream ofs(filename, ios::binary);
        if (!ofs) throw runtime_error("Cannot open file for writing");

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
                    const string& val = std::get<string>(field);
                    int len = static_cast<int>(val.size());
                    ofs.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    ofs.write(val.c_str(), len);
                }
            }
        }
    }

    void load(const string& filename) {
        ifstream ifs(filename, ios::binary);
        if (!ifs) return;

        data.clear();
        if (stringIndex) stringIndex->clear();

        size_t schemaSize;
        ifs.read(reinterpret_cast<char*>(&schemaSize), sizeof(schemaSize));
        if (schemaSize != schema.size()) throw runtime_error("Schema mismatch");

        for (size_t i = 0; i < schemaSize; ++i) {
            int nameLen;
            ifs.read(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));
            string name(nameLen, '\0');
            ifs.read(&name[0], nameLen);
            ColumnType type;
            ifs.read(reinterpret_cast<char*>(&type), sizeof(type));
            if (name != schema[i].name || type != schema[i].type) throw runtime_error("Schema mismatch");
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
                    string val(len, '\0');
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
        if (row.size() != schema.size()) throw runtime_error("Row size does not match schema");
        for (size_t i = 0; i < schema.size(); ++i) {
            if ((schema[i].type == ColumnType::INT && !std::holds_alternative<int>(row[i])) ||
                (schema[i].type == ColumnType::STRING && !std::holds_alternative<string>(row[i]))) {
                throw runtime_error("Type mismatch in row");
            }
        }
    }

    void updateIndex(int id, const Row& row) {
        if (stringIndex && indexedColumn > 0) {
            const string& value = std::get<string>(row[indexedColumn]);
            (*stringIndex)[value].insert(id);
        }
    }

    void removeFromIndex(int id) {
        if (stringIndex && indexedColumn > 0) {
            auto rowIt = data.find(id);
            if (rowIt != data.end()) {
                const string& value = std::get<string>(rowIt->second[indexedColumn]);
                (*stringIndex)[value].erase(id);
                if ((*stringIndex)[value].empty()) stringIndex->erase(value);
            }
        }
    }
};

class Database {
private:
    unordered_map<string, unique_ptr<Table>> databases;
    Table* currentDb = nullptr;
    string currentDbName;

    enum class TokenType { KEYWORD, INT, STRING, PUNCTUATION, IDENTIFIER };

    struct Token {
        TokenType type;
        string value;
    };

    vector<Token> tokenize(const string& input) {
        vector<Token> tokens;
        string token;
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
            } else if (isspace(c)) {
                if (!token.empty()) {
                    addToken(tokens, token);
                    token.clear();
                }
            } else if (c == '(' || c == ')' || c == ',' || c == '=') {
                if (!token.empty()) {
                    addToken(tokens, token);
                    token.clear();
                }
                tokens.push_back({TokenType::PUNCTUATION, string(1, c)});
            } else {
                token += c;
            }
        }
        if (!token.empty()) addToken(tokens, token);
        return tokens;
    }

    void addToken(vector<Token>& tokens, const string& token) {
        static const unordered_set<string> keywords = {
            "CREATE", "DATABASE", "USE", "SHOW", "DATABASES",
            "DROP", "INSERT", "INTO", "VALUES",
            "SELECT", "FROM", "WHERE", "UPDATE", "SET", "DELETE"
        };

        if (keywords.find(token) != keywords.end()) {
            tokens.push_back({TokenType::KEYWORD, token});
        } else if (all_of(token.begin(), token.end(), ::isdigit)) {
            tokens.push_back({TokenType::INT, token});
        } else {
            tokens.push_back({TokenType::IDENTIFIER, token});
        }
    }

    void parseCreate(const vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "CREATE", TokenType::KEYWORD);
        expect(tokens, i++, "DATABASE", TokenType::KEYWORD);
        if (i >= tokens.size() || tokens[i].type != TokenType::IDENTIFIER) {
            throw runtime_error("Expected database name after 'CREATE DATABASE'");
        }
        string dbName = tokens[i++].value;
        if (databases.find(dbName) != databases.end()) {
            throw runtime_error("Database '" + dbName + "' already exists");
        }
        databases[dbName] = make_unique<Table>(
            vector<Column>{{"id", ColumnType::INT}, {"name", ColumnType::STRING}});
        databases[dbName]->load(dbName + ".dat"); // Load if exists
        databases[dbName]->createIndex(1);
        currentDb = databases[dbName].get();
        currentDbName = dbName;
        cout << "Database '" << dbName << "' created and selected" << endl << flush;
    }

    void parseUse(const vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "USE", TokenType::KEYWORD);
        if (i >= tokens.size() || tokens[i].type != TokenType::IDENTIFIER) {
            throw runtime_error("Expected database name after 'USE'");
        }
        string dbName = tokens[i++].value;
        if (databases.find(dbName) == databases.end()) {
            throw runtime_error("Database '" + dbName + "' does not exist");
        }
        currentDb = databases[dbName].get();
        currentDbName = dbName;
        cout << "Switched to database '" << dbName << "'" << endl << flush;
    }

    void parseShowDatabases(const vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "SHOW", TokenType::KEYWORD);
        expect(tokens, i++, "DATABASES", TokenType::KEYWORD);
        if (i != tokens.size()) throw runtime_error("Extra tokens after 'SHOW DATABASES'");
        cout << "Databases:\n";
        for (const auto& pair : databases) {
            cout << "  " << pair.first << (pair.first == currentDbName ? " (current)" : "") << endl;
        }
        cout << flush;
    }

    void parseDropDatabase(const vector<Token>& tokens) {
        size_t i = 0;
        expect(tokens, i++, "DROP", TokenType::KEYWORD);
        expect(tokens, i++, "DATABASE", TokenType::KEYWORD);
        if (i >= tokens.size() || tokens[i].type != TokenType::IDENTIFIER) {
            throw runtime_error("Expected database name after 'DROP DATABASE'");
        }
        string dbName = tokens[i++].value;
        if (databases.find(dbName) == databases.end()) {
            throw runtime_error("Database '" + dbName + "' does not exist");
        }
        databases.erase(dbName);
        if (dbName == currentDbName) {
            currentDb = nullptr;
            currentDbName.clear();
            cout << "Database '" << dbName << "' dropped. No database selected." << endl << flush;
        } else {
            cout << "Database '" << dbName << "' dropped" << endl << flush;
        }
    }

    void parseInsert(const vector<Token>& tokens) {
        if (!currentDb) throw runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
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
                row.push_back(stoi(tokens[i - 1].value));
            } else {
                expect(tokens, i++, TokenType::STRING);
                row.push_back(tokens[i - 1].value);
            }
        }
        expect(tokens, i++, ")", TokenType::PUNCTUATION);
        currentDb->insert(row);
        cout << "Inserted successfully into '" << currentDbName << "'" << endl << flush;
    }

    void parseSelect(const vector<Token>& tokens) {
        if (!currentDb) throw runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
        size_t i = 0;
        expect(tokens, i++, "SELECT", TokenType::KEYWORD);
        expect(tokens, i++, "*", TokenType::IDENTIFIER);
        expect(tokens, i++, "FROM", TokenType::KEYWORD);
        expect(tokens, i++, "table", TokenType::IDENTIFIER);
        expect(tokens, i++, "WHERE", TokenType::KEYWORD);
        expect(tokens, i++, "id", TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        if (i >= tokens.size() || tokens[i].type != TokenType::INT) {
            throw runtime_error("Expected integer value after 'id =' (e.g., 1)");
        }
        expect(tokens, i++, TokenType::INT);
        int id = stoi(tokens[i - 1].value);
        if (i != tokens.size()) throw runtime_error("Extra tokens after command");
        Row row = currentDb->get(id);
        for (size_t j = 0; j < row.size(); ++j) {
            visit([](const auto& v) { cout << v; }, row[j]);
            if (j < row.size() - 1) cout << ", ";
        }
        cout << endl << flush;
    }

    void parseUpdate(const vector<Token>& tokens) {
        if (!currentDb) throw runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
        size_t i = 0;
        expect(tokens, i++, "UPDATE", TokenType::KEYWORD);
        expect(tokens, i++, "table", TokenType::IDENTIFIER);
        expect(tokens, i++, "SET", TokenType::KEYWORD);
        expect(tokens, i++, currentDb->getSchema()[1].name, TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        expect(tokens, i++, TokenType::STRING);
        string newValue = tokens[i - 1].value;
        expect(tokens, i++, "WHERE", TokenType::KEYWORD);
        expect(tokens, i++, "id", TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        expect(tokens, i++, TokenType::INT);
        int id = stoi(tokens[i - 1].value);
        Row row = currentDb->get(id);
        row[1] = newValue;
        currentDb->update(id, row);
        cout << "Updated successfully in '" << currentDbName << "'" << endl << flush;
    }

    void parseDelete(const vector<Token>& tokens) {
        if (!currentDb) throw runtime_error("No database selected. Use 'CREATE DATABASE' or 'USE'");
        size_t i = 0;
        expect(tokens, i++, "DELETE", TokenType::KEYWORD);
        expect(tokens, i++, "FROM", TokenType::KEYWORD);
        expect(tokens, i++, "table", TokenType::IDENTIFIER);
        expect(tokens, i++, "WHERE", TokenType::KEYWORD);
        expect(tokens, i++, "id", TokenType::IDENTIFIER);
        expect(tokens, i++, "=", TokenType::PUNCTUATION);
        expect(tokens, i++, TokenType::INT);
        int id = stoi(tokens[i - 1].value);
        currentDb->remove(id);
        cout << "Deleted successfully from '" << currentDbName << "'" << endl << flush;
    }

    void expect(const vector<Token>& tokens, size_t i, const string& value, TokenType type) {
        if (i >= tokens.size() || tokens[i].type != type || tokens[i].value != value) {
            throw runtime_error("Syntax error at token " + to_string(i));
        }
    }

    void expect(const vector<Token>& tokens, size_t i, TokenType type) {
        if (i >= tokens.size() || tokens[i].type != type) {
            throw runtime_error("Syntax error at token " + to_string(i));
        }
    }

    void execute(const string& command) {
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
        else throw runtime_error("Unknown command: " + tokens[0].value);
    }

public:
    Database() = default;

    ~Database() {
        for (auto& pair : databases) {
            pair.second->save(pair.first + ".dat");
        }
    }

    void run() {
        cout << "Custom Database Engine\nCommands:\n"
             << "  CREATE DATABASE dbname\n"
             << "  USE dbname\n"
             << "  SHOW DATABASES\n"
             << "  DROP DATABASE dbname\n"
             << "  INSERT INTO table VALUES (1, 'name')\n"
             << "  SELECT * FROM table WHERE id = 1\n"
             << "  UPDATE table SET name = 'newname' WHERE id = 1\n"
             << "  DELETE FROM table WHERE id = 1\n"
             << "  EXIT to quit\n";

        string line;
        while (true) {
            cout << (currentDbName.empty() ? "No DB> " : currentDbName + "> ") << flush;
            if (!getline(cin, line)) break; // Handle Ctrl+D or EOF gracefully
            if (line == "EXIT") break;
            try {
                execute(line);
            } catch (const exception& e) {
                cerr << "Error: " << e.what() << endl;
            }
        }
    }
};

int main() {
    try {
        Database db;
        db.run();
    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return 1;
    }
    return 0;
}
