package monitor

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"log"
	"strings"
	"time"
)

type LogEntry struct {
	ID        int
	Timestamp string
	URL       string
	Method    string
	Status    int64
	ReqBody   []byte
	RespBody  []byte
}

type DBStore struct {
	db *sql.DB
}

func NewDBStore(dbPath string) (*DBStore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	query := `
	CREATE TABLE IF NOT EXISTS network_traffic (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		url TEXT,
		method TEXT,
		status INTEGER,
		req_body BLOB,
		resp_body BLOB
	);`

	if _, err := db.Exec(query); err != nil {
		return nil, err
	}

	return &DBStore{db: db}, nil
}

func (s *DBStore) Add(call *HTTPCall) {
	// compress the payloads with gzip
	compressedReq := compressBytes(call.ReqBody)
	compressedResp := compressBytes(call.RespBody)

	query := `INSERT INTO network_traffic (url, method, status, req_body, resp_body) VALUES (?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, call.URL, call.Method, call.Status, compressedReq, compressedResp)
	if err != nil {
		log.Printf("Database insert error: %v\n", err)
	}
}

func compressBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(data)
	w.Close()
	return b.Bytes()
}

func InitializeDB() *DBStore {
	dbStore, err := NewDBStore("./traffic.db")
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	return dbStore
}

func decompressBytes(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var b bytes.Buffer
	_, err = b.ReadFrom(r)
	return b.Bytes(), err
}

func (s *DBStore) scanLogs(rows *sql.Rows) ([]*LogEntry, error) {
	defer rows.Close()
	var logs []*LogEntry

	for rows.Next() {
		var entry LogEntry
		var reqBlob, respBlob []byte

		err := rows.Scan(
			&entry.ID,
			&entry.Timestamp,
			&entry.URL,
			&entry.Method,
			&entry.Status,
			&reqBlob,
			&respBlob,
		)
		if err != nil {
			return nil, err
		}

		entry.ReqBody, _ = decompressBytes(reqBlob)
		entry.RespBody, _ = decompressBytes(respBlob)

		logs = append(logs, &entry)
	}

	return logs, rows.Err()
}

// get logs with pagination
func (s *DBStore) GetLogs(limit, offset int) ([]*LogEntry, error) {
	query := `
		SELECT id, timestamp, url, method, status, req_body, resp_body 
		FROM network_traffic 
		ORDER BY timestamp DESC 
		LIMIT ? OFFSET ?`

	rows, err := s.db.Query(query, limit, offset)
	if err != nil {
		return nil, err
	}
	return s.scanLogs(rows)
}

// get logs in time range
func (s *DBStore) GetLogsByTimeRange(start, end time.Time) ([]*LogEntry, error) {
	// time.Time to match DATETIME format in SQLite
	startStr := start.Format("2006-01-02 15:04:05")
	endStr := end.Format("2006-01-02 15:04:05")

	query := `
		SELECT id, timestamp, url, method, status, req_body, resp_body 
		FROM network_traffic 
		WHERE timestamp BETWEEN ? AND ? 
		ORDER BY timestamp DESC`

	rows, err := s.db.Query(query, startStr, endStr)
	if err != nil {
		return nil, err
	}
	return s.scanLogs(rows)
}

// get logs by status code
func (s *DBStore) GetLogsByStatus(status int) ([]*LogEntry, error) {
	query := `
		SELECT id, timestamp, url, method, status, req_body, resp_body 
		FROM network_traffic 
		WHERE status = ? 
		ORDER BY timestamp DESC`

	rows, err := s.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	return s.scanLogs(rows)
}

func (s *DBStore) GetLogsByURL(urlPattern string) ([]*LogEntry, error) {
	query := `
		SELECT id, timestamp, url, method, status, req_body, resp_body 
		FROM network_traffic 
		WHERE url LIKE ? 
		ORDER BY timestamp DESC`

	if !strings.Contains(urlPattern, "%") {
		urlPattern = "%" + urlPattern + "%"
	}

	rows, err := s.db.Query(query, urlPattern)
	if err != nil {
		return nil, err
	}
	return s.scanLogs(rows)
}

// get all unique HTTP status codes
func (s *DBStore) GetUniqueStatuses() ([]int64, error) {
	query := `SELECT DISTINCT status FROM network_traffic WHERE status IS NOT NULL ORDER BY status ASC`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statuses []int64
	for rows.Next() {
		var status int64
		if err := rows.Scan(&status); err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}

	return statuses, rows.Err()
}

// get all unique urls
func (s *DBStore) GetUniqueURLs() ([]string, error) {
	query := `SELECT DISTINCT url FROM network_traffic WHERE url IS NOT NULL ORDER BY url ASC`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, err
		}
		urls = append(urls, url)
	}

	return urls, rows.Err()
}

// get logs time range
// returns [OldestTimestamp, NewestTimestamp]
func (s *DBStore) GetTimeRange() ([]string, error) {
	query := `SELECT MIN(timestamp), MAX(timestamp) FROM network_traffic`

	var oldest, newest sql.NullString
	err := s.db.QueryRow(query).Scan(&oldest, &newest)
	if err != nil {
		return nil, err
	}

	if !oldest.Valid || !newest.Valid {
		return []string{}, nil
	}

	return []string{oldest.String, newest.String}, nil
}

// delete all logs
func (s *DBStore) DeleteAllLogs() (int64, error) {
	result, err := s.db.Exec(`DELETE FROM network_traffic`)
	if err != nil {
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()

	// rebuild the database file to physically reclaim disk space!
	_, err = s.db.Exec(`VACUUM`)
	if err != nil {
		log.Printf("Warning: Failed to vacuum database: %v\n", err)
	}

	return rowsAffected, nil
}

func (s *DBStore) DeleteLogsAfter(cutoff time.Time) (int64, error) {
	cutoffStr := cutoff.Format("2006-01-02 15:04:05")

	query := `DELETE FROM network_traffic WHERE timestamp < ?`
	result, err := s.db.Exec(query, cutoffStr)
	if err != nil {
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()

	_, err = s.db.Exec(`VACUUM`)
	if err != nil {
		log.Printf("Warning: Failed to vacuum database: %v\n", err)
	}

	return rowsAffected, nil
}

func (s *DBStore) GetDatabaseSizeBytes() (int64, error) {
	var pageCount, pageSize int64
	if err := s.db.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		return 0, err
	}
	if err := s.db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		return 0, err
	}
	totalBytes := pageCount * pageSize

	return totalBytes, nil
}
