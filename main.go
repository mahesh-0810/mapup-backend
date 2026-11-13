package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux" // Add this to your imports at the very top of main.go to convert strings to integers!
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

// The underscore imports the driver anonymously so database/sql can use it

// Global database connection variable
var db *sql.DB

// WebSocket Upgrader: Upgrades a standard HTTP connection to a continuous WebSocket connection
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allows any frontend (like your Next.js app) to connect
	},
}

// Map to keep track of all connected frontend clients
var clients = make(map[*websocket.Conn]bool)

// 1. Struct for the incoming JSON request
type VehicleRequest struct {
	VehicleNumber string `json:"vehicle_number"`
	DriverName    string `json:"driver_name"`
	VehicleType   string `json:"vehicle_type"`
	Phone         string `json:"phone"`
}

// 2. Struct for the outgoing JSON response
type VehicleResponse struct {
	ID            string `json:"id"`
	VehicleNumber string `json:"vehicle_number"`
	Status        string `json:"status"`
	TimeNs        string `json:"time_ns"`
}

// Represents a single vehicle in the database
type Vehicle struct {
	ID            string    `json:"id"`
	VehicleNumber string    `json:"vehicle_number"`
	DriverName    string    `json:"driver_name"`
	VehicleType   string    `json:"vehicle_type"`
	Phone         string    `json:"phone"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

// Represents the exact JSON response for GET /vehicles
type GetVehiclesResponse struct {
	Vehicles []Vehicle `json:"vehicles"`
	TimeNs   string    `json:"time_ns"`
}

// Struct for incoming Geofence JSON request
type GeofenceRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Category    string      `json:"category"`
	Coordinates [][]float64 `json:"coordinates"` // Array of [lat, lng] arrays
}

// Struct for outgoing Geofence JSON response
type GeofenceResponse struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	TimeNs string `json:"time_ns"`
}

// Struct for a single Geofence in the GET response
type Geofence struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Coordinates [][]float64 `json:"coordinates"`
	Category    string      `json:"category"`
	CreatedAt   time.Time   `json:"created_at"`
}

// Struct for the exact JSON response for GET /geofences
type GetGeofencesResponse struct {
	Geofences []Geofence `json:"geofences"`
	TimeNs    string     `json:"time_ns"`
}

// Helper struct to parse the raw GeoJSON returned by PostGIS
type postgisGeoJSON struct {
	Coordinates [][][]float64 `json:"coordinates"` // GeoJSON wraps polygons in extra arrays
}

// Incoming location update request
type LocationRequest struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp string  `json:"timestamp"`
}

// Information about a geofence the vehicle is currently inside
type CurrentGeofence struct {
	GeofenceID   string `json:"geofence_id"`
	GeofenceName string `json:"geofence_name"`
	Status       string `json:"status"`
}

// Outgoing location update response
type LocationResponse struct {
	VehicleID        string            `json:"vehicle_id"`
	LocationUpdated  bool              `json:"location_updated"`
	CurrentGeofences []CurrentGeofence `json:"current_geofences"`
	TimeNs           string            `json:"time_ns"`
}

// Incoming alert configuration request
type AlertRequest struct {
	GeofenceID string `json:"geofence_id"`
	VehicleID  string `json:"vehicle_id,omitempty"` // omitempty allows this to be optional
	EventType  string `json:"event_type"`           // entry, exit, or both
}

// Outgoing alert configuration response
type AlertResponse struct {
	AlertID    string `json:"alert_id"`
	GeofenceID string `json:"geofence_id"`
	VehicleID  string `json:"vehicle_id"`
	EventType  string `json:"event_type"`
	Status     string `json:"status"`
	TimeNs     string `json:"time_ns"`
}

// Struct for a single alert rule in the GET response
type AlertData struct {
	AlertID       string    `json:"alert_id"`
	GeofenceID    string    `json:"geofence_id"`
	GeofenceName  string    `json:"geofence_name"`
	VehicleID     string    `json:"vehicle_id"`
	VehicleNumber string    `json:"vehicle_number"`
	EventType     string    `json:"event_type"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

// Struct for the exact JSON response for GET /alerts
type GetAlertsResponse struct {
	Alerts []AlertData `json:"alerts"`
	TimeNs string      `json:"time_ns"`
}

// Struct for a single historical violation
type Violation struct {
	ID            string  `json:"id"`
	VehicleID     string  `json:"vehicle_id"`
	VehicleNumber string  `json:"vehicle_number"`
	GeofenceID    string  `json:"geofence_id"`
	GeofenceName  string  `json:"geofence_name"`
	EventType     string  `json:"event_type"` // entry or exit
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	Timestamp     string  `json:"timestamp"`
}

// Struct for the GET /violations/history response
type ViolationsHistoryResponse struct {
	Violations []Violation `json:"violations"`
	TotalCount int         `json:"total_count"`
	TimeNs     string      `json:"time_ns"`
}

// getEnv reads an environment variable, falling back to a default for local (non-Docker) runs.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// corsMiddleware allows the frontend (on a different origin once deployed) to call the API.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	// 3. Connect to PostgreSQL. Defaults match docker-compose for local dev;
	// override via env vars when deploying.
	connStr := fmt.Sprintf(
		"user=%s password=%s dbname=%s sslmode=%s host=%s port=%s",
		getEnv("DB_USER", "postgres"),
		getEnv("DB_PASSWORD", "root"),
		getEnv("DB_NAME", "geofence_db"),
		getEnv("DB_SSLMODE", "disable"),
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_PORT", "5432"),
	)
	var err error

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to open DB connection: ", err)
	}
	// defer ensures the connection closes when the server stops running
	defer db.Close()

	// Retry the initial connection: Postgres (especially on first boot, while it
	// runs docker-entrypoint-initdb.d scripts) can report healthy and then briefly
	// restart, so a single Ping() right at container startup can race it.
	maxAttempts := 10
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = db.Ping(); err == nil {
			break
		}
		log.Printf("Database not ready yet (attempt %d/%d): %v\n", attempt, maxAttempts, err)
		if attempt == maxAttempts {
			log.Fatal("Database is unreachable: ", err)
		}
		time.Sleep(3 * time.Second)
	}
	fmt.Println("Connected to PostGIS successfully!")

	// 4. Set up the Gorilla Mux router
	router := mux.NewRouter()

	// Bind the POST /vehicles endpoint to our handler function
	router.HandleFunc("/vehicles", createVehicleHandler).Methods("POST")

	// Bind the GET /vehicles endpoint
	router.HandleFunc("/vehicles", getVehiclesHandler).Methods("GET")

	// Bind the POST /geofences endpoint
	router.HandleFunc("/geofences", createGeofenceHandler).Methods("POST")

	// Bind the GET /geofences endpoint
	router.HandleFunc("/geofences", getGeofencesHandler).Methods("GET")

	// Bind the POST /vehicles/location endpoint
	router.HandleFunc("/vehicles/location", updateLocationHandler).Methods("POST")

	// Bind the POST /alerts/configure endpoint
	router.HandleFunc("/alerts/configure", configureAlertHandler).Methods("POST")

	// Bind the GET /alerts endpoint
	router.HandleFunc("/alerts", getAlertsHandler).Methods("GET")

	// Bind the GET /violations/history endpoint
	router.HandleFunc("/violations/history", getViolationsHandler).Methods("GET")

	// Bind the WebSocket endpoint
	router.HandleFunc("/ws", wsHandler)

	// 5. Start the HTTP server
	port := getEnv("PORT", "8080")
	fmt.Printf("Server running on http://localhost:%s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, corsMiddleware(router)))
}

// Handler function for POST /vehicles
func createVehicleHandler(w http.ResponseWriter, r *http.Request) {
	// 6. Start the timer immediately to fulfill the assessment requirement
	startTime := time.Now()

	// 7. Parse the incoming JSON body into our Go struct
	var req VehicleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 8. Generate a unique ID (e.g., veh_a1b2c3d4)
	newID := "veh_" + uuid.New().String()[:8]

	// 9. Execute the raw SQL INSERT command using parameterized queries ($1, $2) to prevent SQL injection
	insertQuery := `
		INSERT INTO vehicles (id, vehicle_number, driver_name, vehicle_type, phone) 
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := db.Exec(insertQuery, newID, req.VehicleNumber, req.DriverName, req.VehicleType, req.Phone)
	if err != nil {
		log.Printf("DB Insert Error: %v\n", err)
		http.Error(w, "Failed to insert vehicle into database", http.StatusInternalServerError)
		return
	}

	// 10. Stop the timer and calculate execution time in nanoseconds
	duration := time.Since(startTime).Nanoseconds()

	// 11. Build the exact JSON response required by the assessment
	res := VehicleResponse{
		ID:            newID,
		VehicleNumber: req.VehicleNumber,
		Status:        "active",
		TimeNs:        fmt.Sprintf("%d", duration),
	}

	// 12. Send the JSON response to the client
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func getVehiclesHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Start the nanosecond timer
	startTime := time.Now()

	// 2. Execute the SELECT query
	rows, err := db.Query("SELECT id, vehicle_number, driver_name, vehicle_type, phone, status, created_at FROM vehicles")
	if err != nil {
		log.Printf("DB Query Error: %v\n", err)
		http.Error(w, "Failed to fetch vehicles", http.StatusInternalServerError)
		return
	}
	defer rows.Close() // Ensure rows are closed after we finish reading them

	// 3. Create a slice (array) to hold our vehicles
	// We initialize it as an empty slice so it returns [] instead of null if the DB is empty
	vehicles := []Vehicle{}

	// 4. Loop through the database rows
	for rows.Next() {
		var v Vehicle
		// 5. Scan the row data into our Vehicle struct variables
		if err := rows.Scan(&v.ID, &v.VehicleNumber, &v.DriverName, &v.VehicleType, &v.Phone, &v.Status, &v.CreatedAt); err != nil {
			log.Printf("Row Scan Error: %v\n", err)
			continue // skip this row if there's an error
		}
		// 6. Append the vehicle to our slice
		vehicles = append(vehicles, v)
	}

	// 7. Calculate execution time
	duration := time.Since(startTime).Nanoseconds()

	// 8. Build the final response
	res := GetVehiclesResponse{
		Vehicles: vehicles,
		TimeNs:   fmt.Sprintf("%d", duration),
	}

	// 9. Send the JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Handler function for POST /geofences
func createGeofenceHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Start the nanosecond timer
	startTime := time.Now()

	// 2. Parse the incoming JSON body
	var req GeofenceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 3. Validate coordinates (minimum 4 points for a closed polygon)
	if len(req.Coordinates) < 4 {
		http.Error(w, "Geofence must have at least 4 coordinates (closed polygon)", http.StatusBadRequest)
		return
	}

	// 4. Convert the JSON array into a PostGIS WKT (Well-Known Text) string
	// WKT format for polygons is: POLYGON((longitude latitude, longitude latitude, ...))
	wkt := "POLYGON(("
	for i, coord := range req.Coordinates {
		// PostGIS expects Longitude FIRST, then Latitude (X then Y).
		// Our JSON gives [Latitude, Longitude], so we swap them: coord[1] coord[0]
		wkt += fmt.Sprintf("%f %f", coord[1], coord[0])

		if i < len(req.Coordinates)-1 {
			wkt += ", "
		}
	}
	wkt += "))"

	// 5. Generate unique ID
	newID := "geo_" + uuid.New().String()[:8]

	// 6. Execute the SQL Insert using ST_GeomFromText to parse the polygon string
	insertQuery := `
		INSERT INTO geofences (id, name, description, category, boundaries) 
		VALUES ($1, $2, $3, $4, ST_GeomFromText($5, 4326))
	`
	_, err := db.Exec(insertQuery, newID, req.Name, req.Description, req.Category, wkt)
	if err != nil {
		log.Printf("DB Insert Error: %v\n", err)
		http.Error(w, "Failed to insert geofence", http.StatusInternalServerError)
		return
	}

	// 7. Calculate execution time
	duration := time.Since(startTime).Nanoseconds()

	// 8. Build and send the JSON response
	res := GeofenceResponse{
		ID:     newID,
		Name:   req.Name,
		Status: "active",
		TimeNs: fmt.Sprintf("%d", duration),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Handler function for GET /geofences
func getGeofencesHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Start the nanosecond timer
	startTime := time.Now()

	// 2. Check for the optional 'category' query parameter (like req.query.category in Express)
	categoryFilter := r.URL.Query().Get("category")

	// 3. Build the SQL query
	// We use ST_AsGeoJSON to ask PostGIS to convert the binary polygon into a readable JSON string
	query := `
		SELECT id, name, description, category, created_at, ST_AsGeoJSON(boundaries) as geojson 
		FROM geofences
	`
	var rows *sql.Rows
	var err error

	// If the user provided a category filter, append a WHERE clause
	if categoryFilter != "" {
		query += ` WHERE category = $1`
		rows, err = db.Query(query, categoryFilter)
	} else {
		rows, err = db.Query(query)
	}

	if err != nil {
		log.Printf("DB Query Error: %v\n", err)
		http.Error(w, "Failed to fetch geofences", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	geofences := []Geofence{}

	// 4. Loop through the rows
	for rows.Next() {
		var g Geofence
		var geojsonStr string

		// Scan the columns into our variables
		if err := rows.Scan(&g.ID, &g.Name, &g.Description, &g.Category, &g.CreatedAt, &geojsonStr); err != nil {
			log.Printf("Row Scan Error: %v\n", err)
			continue
		}

		// 5. Parse the GeoJSON string returned by PostGIS
		geo := postgisGeoJSON{}
		json.Unmarshal([]byte(geojsonStr), &geo)

		// 6. Extract the coordinates and flip them back from [Lng, Lat] to [Lat, Lng]
		var formattedCoords [][]float64
		if len(geo.Coordinates) > 0 {
			for _, coord := range geo.Coordinates[0] {
				// GeoJSON returns [Longitude, Latitude], but the assignment requires [Latitude, Longitude]
				latLng := []float64{coord[1], coord[0]}
				formattedCoords = append(formattedCoords, latLng)
			}
		}
		g.Coordinates = formattedCoords

		geofences = append(geofences, g)
	}

	// 7. Calculate execution time
	duration := time.Since(startTime).Nanoseconds()

	// 8. Build the final response
	res := GetGeofencesResponse{
		Geofences: geofences,
		TimeNs:    fmt.Sprintf("%d", duration),
	}

	// 9. Send the JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Handler function for POST /vehicles/location
func updateLocationHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Start the nanosecond timer
	startTime := time.Now()

	var req LocationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var vehicleNumber string
	db.QueryRow("SELECT vehicle_number FROM vehicles WHERE id = $1", req.VehicleID).Scan(&vehicleNumber)

	// 2. Find which geofences the vehicle was inside BEFORE this update, based on
	// its most recent previously-recorded location (needed to detect "exit" events).
	previousGeofences := make(map[string]bool)
	var prevLat, prevLng sql.NullFloat64
	err := db.QueryRow(`
		SELECT ST_Y(location), ST_X(location)
		FROM vehicle_locations
		WHERE vehicle_id = $1
		ORDER BY recorded_at DESC
		LIMIT 1
	`, req.VehicleID).Scan(&prevLat, &prevLng)
	if err == nil && prevLat.Valid && prevLng.Valid {
		prevRows, perr := db.Query(`
			SELECT id FROM geofences
			WHERE ST_Contains(boundaries, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		`, prevLng.Float64, prevLat.Float64)
		if perr == nil {
			for prevRows.Next() {
				var gid string
				if prevRows.Scan(&gid) == nil {
					previousGeofences[gid] = true
				}
			}
			prevRows.Close()
		}
	}

	// 3. Store the new location in the database using PostGIS ST_SetSRID and ST_MakePoint
	insertQuery := `
		INSERT INTO vehicle_locations (vehicle_id, location, recorded_at)
		VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326), $4)
	`
	_, err = db.Exec(insertQuery, req.VehicleID, req.Longitude, req.Latitude, req.Timestamp)
	if err != nil {
		log.Printf("Location Insert Error: %v\n", err)
		http.Error(w, "Failed to save location", http.StatusInternalServerError)
		return
	}

	// 4. The Core Geospatial Query: Check which geofences this point is currently inside
	checkQuery := `
		SELECT id, name
		FROM geofences
		WHERE ST_Contains(boundaries, ST_SetSRID(ST_MakePoint($1, $2), 4326))
	`
	rows, err := db.Query(checkQuery, req.Longitude, req.Latitude)
	if err != nil {
		log.Printf("Geofence Check Error: %v\n", err)
		http.Error(w, "Failed to check geofences", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// 5. Initialize an empty slice for our current geofences
	activeGeofences := []CurrentGeofence{}
	currentGeofences := make(map[string]bool)

	for rows.Next() {
		var cg CurrentGeofence
		if err := rows.Scan(&cg.GeofenceID, &cg.GeofenceName); err != nil {
			continue
		}
		cg.Status = "inside"
		activeGeofences = append(activeGeofences, cg)
		currentGeofences[cg.GeofenceID] = true
	}

	// 6. Diff previous vs. current geofences to find entries (newly inside) and exits (just left)
	enteredGeofences := make(map[string]bool)
	for gid := range currentGeofences {
		if !previousGeofences[gid] {
			enteredGeofences[gid] = true
		}
	}
	exitedGeofences := make(map[string]bool)
	for gid := range previousGeofences {
		if !currentGeofences[gid] {
			exitedGeofences[gid] = true
		}
	}

	// --- LIVE ALERT LOGIC: fire entry/exit alerts for any matching active rule ---

	rulesQuery := `
		SELECT ar.id, ar.geofence_id, ar.event_type, g.name
		FROM alert_rules ar
		JOIN geofences g ON ar.geofence_id = g.id
		WHERE ar.status = 'active' AND (ar.vehicle_id = $1 OR ar.vehicle_id = '' OR ar.vehicle_id IS NULL)
	`
	ruleRows, err := db.Query(rulesQuery, req.VehicleID)
	if err == nil {
		defer ruleRows.Close()

		for ruleRows.Next() {
			var ruleID, ruleGeoID, eventType, geoName string
			ruleRows.Scan(&ruleID, &ruleGeoID, &eventType, &geoName)

			firedEventType := ""
			if enteredGeofences[ruleGeoID] && (eventType == "entry" || eventType == "both") {
				firedEventType = "entry"
			} else if exitedGeofences[ruleGeoID] && (eventType == "exit" || eventType == "both") {
				firedEventType = "exit"
			}
			if firedEventType == "" {
				continue
			}

			violationID := "viol_" + uuid.New().String()[:8]

			insertViol := `
				INSERT INTO violations (id, vehicle_id, geofence_id, event_type, latitude, longitude, timestamp)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
			`
			db.Exec(insertViol, violationID, req.VehicleID, ruleGeoID, firedEventType, req.Latitude, req.Longitude, req.Timestamp)

			alertPayload := Violation{
				ID:            violationID,
				VehicleID:     req.VehicleID,
				VehicleNumber: vehicleNumber,
				GeofenceID:    ruleGeoID,
				GeofenceName:  geoName,
				EventType:     firedEventType,
				Latitude:      req.Latitude,
				Longitude:     req.Longitude,
				Timestamp:     req.Timestamp,
			}

			go broadcastAlert(alertPayload)
		}
	}
	// --- END LIVE ALERT LOGIC ---

	// 5. Calculate execution time
	duration := time.Since(startTime).Nanoseconds()

	// 6. Build the response
	res := LocationResponse{
		VehicleID:        req.VehicleID,
		LocationUpdated:  true,
		CurrentGeofences: activeGeofences,
		TimeNs:           fmt.Sprintf("%d", duration),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Handler function for POST /alerts/configure
func configureAlertHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Start the nanosecond timer
	startTime := time.Now()

	// 2. Parse the incoming JSON body
	var req AlertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 3. Validate the event_type requirement
	if req.EventType != "entry" && req.EventType != "exit" && req.EventType != "both" {
		http.Error(w, "event_type must be entry, exit, or both", http.StatusBadRequest)
		return
	}

	// 4. Generate a unique ID for the alert rule
	newID := "alert_" + uuid.New().String()[:8]

	// 5. Insert the rule into the database
	insertQuery := `
		INSERT INTO alert_rules (id, geofence_id, vehicle_id, event_type)
		VALUES ($1, $2, $3, $4)
	`
	// If vehicle_id is missing from the JSON, Go defaults it to an empty string.
	// In SQL, we will store that empty string to represent "all vehicles".
	_, err := db.Exec(insertQuery, newID, req.GeofenceID, req.VehicleID, req.EventType)
	if err != nil {
		log.Printf("Alert Insert Error: %v\n", err)
		http.Error(w, "Failed to configure alert", http.StatusInternalServerError)
		return
	}

	// 6. Calculate execution time
	duration := time.Since(startTime).Nanoseconds()

	// 7. Build the response
	res := AlertResponse{
		AlertID:    newID,
		GeofenceID: req.GeofenceID,
		VehicleID:  req.VehicleID,
		EventType:  req.EventType,
		Status:     "active",
		TimeNs:     fmt.Sprintf("%d", duration),
	}

	// 8. Send the JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Handler function for GET /alerts
func getAlertsHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Start the timer
	startTime := time.Now()

	// 2. Extract optional query parameters
	geoFilter := r.URL.Query().Get("geofence_id")
	vehFilter := r.URL.Query().Get("vehicle_id")

	// 3. Build the SQL query using JOINs to get names and numbers
	// We use LEFT JOIN on vehicles because the vehicle_id in an alert rule is optional
	query := `
		SELECT 
			a.id, a.geofence_id, g.name AS geofence_name, 
			a.vehicle_id, v.vehicle_number, 
			a.event_type, a.status, a.created_at
		FROM alert_rules a
		JOIN geofences g ON a.geofence_id = g.id
		LEFT JOIN vehicles v ON a.vehicle_id = v.id
		WHERE 1=1
	`

	// Slices to hold dynamic query arguments (to prevent SQL injection)
	args := []interface{}{}
	argCounter := 1

	// 4. Dynamically append filters if the user provided them in the URL
	if geoFilter != "" {
		query += fmt.Sprintf(" AND a.geofence_id = $%d", argCounter)
		args = append(args, geoFilter)
		argCounter++
	}
	if vehFilter != "" {
		query += fmt.Sprintf(" AND a.vehicle_id = $%d", argCounter)
		args = append(args, vehFilter)
		argCounter++
	}

	// 5. Execute the query
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("DB Query Error: %v\n", err)
		http.Error(w, "Failed to fetch alerts", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	alerts := []AlertData{}

	// 6. Loop through rows and handle potential NULL values
	for rows.Next() {
		var a AlertData
		var vehID, vehNum sql.NullString // Handles the case where vehicle data might be empty

		err := rows.Scan(
			&a.AlertID, &a.GeofenceID, &a.GeofenceName,
			&vehID, &vehNum,
			&a.EventType, &a.Status, &a.CreatedAt,
		)
		if err != nil {
			log.Printf("Row Scan Error: %v\n", err)
			continue
		}

		// Convert the safe NullString back to a regular string for JSON
		if vehID.Valid {
			a.VehicleID = vehID.String
		}
		if vehNum.Valid {
			a.VehicleNumber = vehNum.String
		}

		alerts = append(alerts, a)
	}

	// 7. Calculate time and build response
	duration := time.Since(startTime).Nanoseconds()

	res := GetAlertsResponse{
		Alerts: alerts,
		TimeNs: fmt.Sprintf("%d", duration),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Handler function for GET /violations/history
func getViolationsHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Start the nanosecond timer
	startTime := time.Now()

	// 2. Extract query parameters
	vehFilter := r.URL.Query().Get("vehicle_id")
	geoFilter := r.URL.Query().Get("geofence_id")
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")
	limitStr := r.URL.Query().Get("limit")

	// 3. Handle Pagination Logic (Default: 50, Max: 500)
	limit := 50
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
			if limit > 500 {
				limit = 500
			}
		}
	}

	// 4. Build the core query and arguments
	args := []interface{}{}
	argCounter := 1
	whereClause := " WHERE 1=1"

	if vehFilter != "" {
		whereClause += fmt.Sprintf(" AND v.vehicle_id = $%d", argCounter)
		args = append(args, vehFilter)
		argCounter++
	}
	if geoFilter != "" {
		whereClause += fmt.Sprintf(" AND v.geofence_id = $%d", argCounter)
		args = append(args, geoFilter)
		argCounter++
	}
	if startDate != "" {
		whereClause += fmt.Sprintf(" AND v.timestamp >= $%d", argCounter)
		args = append(args, startDate)
		argCounter++
	}
	if endDate != "" {
		whereClause += fmt.Sprintf(" AND v.timestamp <= $%d", argCounter)
		args = append(args, endDate)
		argCounter++
	}

	// 5. Query to get the Total Count of records (ignoring the limit)
	countQuery := "SELECT COUNT(*) FROM violations v" + whereClause
	var totalCount int
	err := db.QueryRow(countQuery, args...).Scan(&totalCount)
	if err != nil {
		log.Printf("Count Query Error: %v\n", err)
		http.Error(w, "Failed to count violations", http.StatusInternalServerError)
		return
	}

	// 6. Main Query to fetch the actual records with JOINs
	query := `
		SELECT 
			v.id, v.vehicle_id, veh.vehicle_number, 
			v.geofence_id, g.name AS geofence_name, 
			v.event_type, v.latitude, v.longitude, v.timestamp
		FROM violations v
		JOIN vehicles veh ON v.vehicle_id = veh.id
		JOIN geofences g ON v.geofence_id = g.id
	` + whereClause + fmt.Sprintf(" ORDER BY v.timestamp DESC LIMIT $%d", argCounter)

	args = append(args, limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("DB Query Error: %v\n", err)
		http.Error(w, "Failed to fetch violations", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	violations := []Violation{}

	for rows.Next() {
		var v Violation
		var timestamp time.Time

		err := rows.Scan(
			&v.ID, &v.VehicleID, &v.VehicleNumber,
			&v.GeofenceID, &v.GeofenceName,
			&v.EventType, &v.Latitude, &v.Longitude, &timestamp,
		)
		if err != nil {
			log.Printf("Row Scan Error: %v\n", err)
			continue
		}

		// Format the timestamp back to standard ISO 8601 string for the JSON response
		v.Timestamp = timestamp.Format(time.RFC3339)
		violations = append(violations, v)
	}

	// 7. Calculate time and build response
	duration := time.Since(startTime).Nanoseconds()

	res := ViolationsHistoryResponse{
		Violations: violations,
		TotalCount: totalCount,
		TimeNs:     fmt.Sprintf("%d", duration),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Handler to establish the WebSocket connection
func wsHandler(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket Upgrade Error:", err)
		return
	}
	defer ws.Close()

	// Register the new client
	clients[ws] = true
	fmt.Println("New WebSocket client connected!")

	// Keep the connection open and listen for disconnections
	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			delete(clients, ws)
			fmt.Println("WebSocket client disconnected")
			break
		}
	}
}

// Helper function to blast alerts to all connected dashboards
func broadcastAlert(alertData Violation) {
	for client := range clients {
		err := client.WriteJSON(alertData)
		if err != nil {
			client.Close()
			delete(clients, client)
		}
	}
}
