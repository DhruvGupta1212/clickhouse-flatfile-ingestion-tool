# ClickHouse & Flat File Ingestion Tool

## Prerequisites
- Docker
- Node.js (v16+ recommended)
- Java (for backend, JDK 11+ recommended)

## 1. Start ClickHouse with a Password

Stop and remove any old ClickHouse containers and volumes:
```sh
docker stop clickhouse-server || true
docker rm clickhouse-server || true
docker volume prune -f
```

Start a fresh ClickHouse container with a password:
```sh
docker run -d --name clickhouse-server --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_PASSWORD=pass clickhouse/clickhouse-server
```

## 2. Test ClickHouse Connection

**In PowerShell:**
```powershell
$headers = @{
    Authorization = ("Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("default:pass")))
}
Invoke-WebRequest -Uri "http://localhost:8123/?query=SHOW%20TABLES" -Headers $headers
```
You should get a 200 OK response (even if no tables).

## 3. Create a Table in ClickHouse
```powershell
Invoke-WebRequest -Uri "http://localhost:8123/?query=CREATE%20TABLE%20test_table%20(id%20UInt32,%20name%20String)%20ENGINE=MergeTree()%20ORDER%20BY%20id" -Headers $headers -Method Post
```

## 4. Start the Backend
```sh
cd backend
./mvnw spring-boot:run
```

## 5. Start the Frontend
```sh
cd frontend
npm install
npm start
```

## 6. Use the Web UI
- Open [http://localhost:3000](http://localhost:3000)
- Fill in the following for ClickHouse:
  - Host: `localhost`
  - Port: `9000` (for native JDBC) or `8123` (if backend is set to use HTTP JDBC)
  - Database: `default`
  - Username: `default`
  - JWT Token/Password: `pass`
- For Flat File, provide a valid CSV path, delimiter, and header option.

## 7. Troubleshooting
- If you get authentication errors, make sure you started ClickHouse with the password and are using the correct port.
- If no tables are fetched, ensure you created a table in ClickHouse.
- Check backend logs for connection errors.

## 8. Stopping Everything
```sh
docker stop clickhouse-server
docker rm clickhouse-server
```

## Features

- Bidirectional data flow (ClickHouse ↔ Flat File)
- JWT token-based authentication for ClickHouse
- Column selection for data ingestion
- Record count reporting
- Schema discovery for both sources
- Efficient data handling with batching/streaming

## Technical Stack

### Backend
- Java 17
- Spring Boot 3.x
- ClickHouse JDBC Driver
- Apache Commons CSV
- JWT Library

### Frontend
- React with TypeScript
- Material-UI
- Axios

## Project Structure

```
clickhouse-flatfile-ingestion-tool/
├── backend/                 # Spring Boot application
│   ├── src/
│   └── pom.xml
├── frontend/               # React application
│   ├── src/
│   └── package.json
└── README.md
```

## Configuration

### ClickHouse Configuration
- Host: Configure in application.properties
- Port: Default 9440/8443 for HTTPS, 9000/8123 for HTTP
- Database: Configure in application.properties
- JWT Token: Provided through UI

### Flat File Configuration
- File Path: Configure in UI
- Delimiters: Configure in UI

## API Documentation

The API documentation will be available at `/swagger-ui.html` when running the application.