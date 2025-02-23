# Custom Database Engine

## Overview
The Custom Database Engine is a lightweight, in-memory database designed for simplicity and efficiency. It provides a command-line interface for basic database operations,
making it a great learning tool for database internals and data management.The entire project is implemented using data structures like vectors,unordered_maps,etc to store and organize data.

## Features

- Support for multiple databases
- Basic CRUD operations (Create, Read, Update, Delete)
- Simple SQL-like query syntax
- In-memory storage for fast access
- Interactive command-line interface

## Technologies Used

- **Programming Language**: C++
- **Libraries**:
  - Standard C++ libraries (`iostream`, `fstream`, `sstream`, `stdexcept`, `string`, `algorithm`)
  - Data structures and containers (`unordered_map`, `unordered_set`, `vector`, `variant`, `memory`)

## Data Structures Used

- **Unordered Map (****`std::unordered_map`****)**: Efficient key-value storage for databases and tables
- **Unordered Set (****`std::unordered_set`****)**: Fast lookups and ensuring unique elements
- **Vector (****`std::vector`****)**: Dynamic storage for table rows and query results
- **Variant (****`std::variant`****)**: Handling multiple data types efficiently
- **Smart Pointers (****`std::unique_ptr`****, ****`std::shared_ptr`****)**: Memory management for dynamic objects

## How to execute
use g++ final.cpp -std=c++17 to execute the file

## Commands
```
Custom Database Engine
Commands:
  CREATE DATABASE dbname
  USE dbname
  SHOW DATABASES
  DROP DATABASE dbname
  INSERT INTO table VALUES (1, 'name')
  SELECT * FROM table WHERE id = 1
  UPDATE table SET name = 'newname' WHERE id = 1
  DELETE FROM table WHERE id = 1
  EXIT to quCustom Database Engine

```

## How It Is Different

- **Lightweight and Fast**: Unlike traditional databases, this engine is designed to be in-memory, making it extremely fast for quick queries.
- **Minimalist SQL-Like Syntax**: Provides an easy-to-use, SQL-inspired command structure without the complexity of full-fledged SQL.
- **Modular and Expandable**: The system is built using modern C++ practices, allowing for easy expansion and integration of new features.

## Future Enhancements

- Persistent storage (saving databases to disk)
- Indexing for faster queries
- Support for complex queries and joins
- Authentication and user roles


