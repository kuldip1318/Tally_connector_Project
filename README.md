---

# Tally Connector

## Description

The Tally Connector integrates with Tally ERP to fetch and sync data (e.g., transactions, ledgers) into a database. It automates data retrieval, storage, and synchronization, enabling businesses to process and analyze Tally data for reporting and decision-making.

## How to Start

### 1. Delete `package-lock.json`
- Before starting, delete the `package-lock.json` file to ensure a fresh installation of dependencies.

### 2. Install Dependencies
- Run the following command to install the required dependencies:
  ```bash
  npm install
  ```

### 3. Start the Application
- First, run the application:
  ```bash
  npm start
  ```
- Then, run the development environment:
  ```bash
  npm run dev
  ```

### 4. Start Tally
- Make sure Tally ERP is running and properly configured.

### 5. Access the Web Interface
- Open your browser and go to:
  ```bash
  http://localhost:9000
  ```

### 6. Sync Data
- Once the web interface loads, you’ll see the list of available Tally companies.
- Subscribe to a company and configure the synchronization settings for data to be fetched at specific intervals.

---
