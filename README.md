# Couchbase Python SDK Samples

Welcome to the `cb_python_sdk_samples` repository! This project provides a collection of sample code demonstrating how to use the [Couchbase Python SDK](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html) to interact with Couchbase Server. These examples are designed as a progressive learning path—starting with simple "Crawl" operations and building up to more complex "Run" scenarios closer to production-ready code.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)
- [Setup and Installation](#setup-and-installation)

## Overview
This repository showcases practical examples for working with Couchbase using the Python SDK. Couchbase is a distributed NoSQL database known for its high performance, scalability, and flexibility. The samples here are structured to guide you through a "Crawl, Walk, Run" progression:
- **Crawl**: Basic operations to get you started.
- **Walk**: Intermediate tasks building on the basics.
- **Run**: More complex scenarios approaching production-grade code.

Whether you're a beginner or an experienced developer, these demos offer ideas and starting points for integrating Couchbase into your Python projects.

## Prerequisites
To use these samples, you’ll need:
- **Couchbase Server**: A running instance (Community or Enterprise Edition). Download it [here](https://www.couchbase.com/downloads).
- **Python**: Version 3.8 or higher.
- A Couchbase bucket, scope, and collection set up with appropriate credentials.

See [Setup and Installation](#setup-and-installation) for detailed setup instructions.

## Examples
The sample scripts in this repository follow a "Crawl, Walk, Run" progression, with each file building on the previous one in complexity and functionality. Below is a detailed breakdown of what each file does:

- **`01_crawl_connect.py`**  
  **Crawl**: This is the simplest starting point. It demonstrates how to establish a connection to a Couchbase cluster using the Python SDK. The script covers basic authentication, connecting to a bucket, and performing a simple "ping" to verify the connection. Perfect for first-time users to ensure their environment is set up correctly.  
  *Key Features*: Connection setup, basic cluster interaction.

- **`02_walk_crud.py`**  
  **Walk**: Builds on the connection basics by introducing CRUD operations (Create, Read, Update, Delete). This script shows how to insert a sample document (e.g., a JSON user profile), retrieve it by key, update a field, and delete it. It’s a step up in complexity, illustrating how to manipulate data in a Couchbase bucket.  
  *Key Features*: Key-value operations, error handling basics.

- **`03_walk_n1ql.py`**  
  **Walk**: Takes querying to the next level with N1QL, Couchbase’s SQL-like query language. This script demonstrates how to run parameterized queries to fetch multiple documents (e.g., all users in a certain city) and process the results. It assumes a basic index is created and introduces query best practices.  
  *Key Features*: N1QL querying, result iteration.

- **`04_run_subdoc.py`**  
  **Run**: Moves toward production-like scenarios by showcasing sub-document operations. This script demonstrates how to update specific fields within a document (e.g., incrementing a counter or updating a nested field) without replacing the entire document. It’s useful for efficient updates in real-world applications.  
  *Key Features*: Sub-document API, partial updates.

- **`05_run_async.py`**  
  **Run**: Introduces asynchronous operations using the Python SDK’s async capabilities. This script performs multiple CRUD or query operations concurrently, simulating a higher-throughput scenario. It’s closer to production code where performance and scalability matter.  
  *Key Features*: Async programming, concurrency handling.

Each script includes detailed comments explaining the code and its purpose. The progression from `01_crawl_connect.py` to `05_run_async.py` mirrors a learning curve from beginner to advanced usage, preparing you for real-world Couchbase applications.

## Contributing
Contributions are welcome! If you have ideas for new samples, improvements, or bug fixes:
1. Fork this repository.
2. Create a new branch (`git checkout -b feature/your-idea`).
3. Make your changes and commit them (`git commit -m "Add my feature"`).
4. Push to your fork (`git push origin feature/your-idea`).
5. Open a pull request.

Please ensure your code follows Python best practices and includes clear comments.

## License
This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute the code as needed.

## Setup and Installation
Follow these steps to run the samples locally:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Fujio-Turner/cb_python_sdk_samples.git
   cd cb_python_sdk_samples
