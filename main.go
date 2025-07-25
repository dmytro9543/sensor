package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"bytes"
	
	"github.com/tarm/serial"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Constants
const (
	MAXNUMADR       = 32
	TRUE            = 1
	FALSE          = 0
	ACK            = 6
	NAK            = 21
	ETX            = 0x03
	DEFAULT_CONFIG = "tempreg.cfg"
	LOCK_FILE      = "tempreg.lck"
	TXBUFFLEN      = 2200
	RXBUFFLEN      = 255
)

// DB configuration
type DBAccessData struct {
	Host   string
	User   string
	Passwd string
	Name   string
}

type SerialPort struct {
	port *serial.Port
}

var db DBAccessData

var configFileName string = ""

// Configuration
var (
	serialDeviceStr      string
	maxRetrys            = 25
	minScanDelaySeconds  = 60.0 // 0 = no delay
	numScans        int64 = 1    // 0 = continuous
	showValues           = true
)

// Device status
var (
	retryCnt      [MAXNUMADR]int
	serNoStr      [MAXNUMADR]string
	valueStr      [MAXNUMADR]string
	scanAddress   [MAXNUMADR]byte
	adrCounter    int
	numAdresses   int
	timestamp     [MAXNUMADR]time.Time
	msgSent       [MAXNUMADR]int64
	msgReceived   [MAXNUMADR]int64
	msgNAK        [MAXNUMADR]int64
	serialPort    *SerialPort
)

var logger *slog.Logger

func main() {
	// Handle cleanup on exit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		cleanup()
		os.Exit(0)
	}()

	// Check for lock file
	if _, err := os.Stat(LOCK_FILE); err == nil {
		log.Fatal("Lock file exists - another instance may be running")
	}

	// Create lock file
	if err := createLockFile(); err != nil {
		log.Fatalf("Failed to create lock file: %v", err)
	}
	defer os.Remove(LOCK_FILE)

	// Parse command line arguments
	parseArgs()

	// Load configuration
	if err := loadConfig(); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize counters
	for i := 0; i < MAXNUMADR; i++ {
		msgSent[i] = 0
		msgReceived[i] = 0
		msgNAK[i] = 0
	}

	// Main loop
	numScansMain := numScans

	var lastScan time.Time

	for numScans == 0 || numScansMain > 0 {

		// Wait for minimum scan delay
		if time.Since(lastScan) < time.Duration(minScanDelaySeconds*float64(time.Second)) {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		if numScansMain > 0 {
			numScansMain--
		}

		// Open serial port
		if err := openPort(serialDeviceStr); err != nil {
			log.Printf("Failed to open port: %v", err)
			continue
		}

		// Dummy read
		if _, _, err := serialPort.ReadStrPort(); err != nil && showValues {
			slog.Error("Dummy read error:", "error", err)
		}

		//scanStart := time.Now()
		
		// Removed unused scanStartT
		for adrCounter = 0; adrCounter < numAdresses; adrCounter++ {
			// Get serial number
			if err := getSerialNumber(); err != nil && showValues {
				slog.Debug("SN Error for address", "address", scanAddress[adrCounter], "error", err)
			}

			// Get measurement
			if err := getMeasurement(); err != nil && showValues {
				slog.Debug("Measurement Error for address", "address", scanAddress[adrCounter], "error", err)
			}

			time.Sleep(100 * time.Millisecond)
		}

		//scanEnd := time.Now()
		//scanDuration = scanEnd.Sub(scanStart)
		lastScan = time.Now()

		// Write to database
		for adrCounter := 0; adrCounter < numAdresses; adrCounter++ {
			if status := writeToPostgres(serNoStr[adrCounter], valueStr[adrCounter], timestamp[adrCounter]); status != 0 {
				if showValues {
					slog.Debug("database write failed", "status", status)
				}
			}
		}

		// Close port
		if err := serialPort.Close(); err != nil {
			slog.Error("Failed to close port", "error", err)
		}
	}
}

func openPort(devStr string) error {
	var err error
	serialPort, err = OpenPort(devStr)
	return err
}

func createLockFile() error {
	file, err := os.Create(LOCK_FILE)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString("running\n")
	return err
}

func parseArgs() {
	if len(os.Args) > 2 {
		configFileName = os.Args[2]
	}

	// Set up command-line flags
	logLevelArg := flag.String("loglevel", "info", "Log level (debug, info, warn, error)")
	flag.Parse()

	// Configure logger
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: parseLogLevel(*logLevelArg),
	}))
	slog.SetDefault(logger) // Make it the default logger
}

func parseLogLevel(levelStr string) slog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo // Default level
	}
}

func loadConfig() error {
	if(configFileName == "") {
		configFileName = DEFAULT_CONFIG
	}
	file, err := os.Open(configFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var scanAddressesStr string

	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.Contains(line, "db.host"):
			db.Host = extractQuotedValue(line)
		case strings.Contains(line, "db.user"):
			db.User = extractQuotedValue(line)
		case strings.Contains(line, "db.passwd"):
			db.Passwd = extractQuotedValue(line)
		case strings.Contains(line, "db.name"):
			db.Name = extractQuotedValue(line)
		case strings.Contains(line, "SerialDevice"):
			serialDeviceStr = extractQuotedValue(line)
		case strings.Contains(line, "minScanDelaySeconds"):
			if val, err := strconv.ParseFloat(extractQuotedValue(line), 64); err == nil {
				minScanDelaySeconds = val
			}
		case strings.Contains(line, "numberOfScans"):
			if val, err := strconv.ParseInt(extractQuotedValue(line), 10, 64); err == nil {
				numScans = val
			}
		case strings.Contains(line, "scanAddresses"):
			scanAddressesStr = extractAddresses(line, scanner)
		}
	}

	if scanAddressesStr != "" {
		extractAdresses(scanAddressesStr)
	} else {
		return errors.New("no scan addresses configured")
	}

	if serialDeviceStr == "" {
		serialDeviceStr = "/dev/ttyUSB0"
	}

	return scanner.Err()
}

func extractQuotedValue(s string) string {
	start := strings.Index(s, "\"")
	if start == -1 {
		return ""
	}
	end := strings.LastIndex(s, "\"")
	if end == -1 || end <= start {
		return ""
	}
	return s[start+1 : end]
}

func extractAddresses(firstLine string, scanner *bufio.Scanner) string {
	result := firstLine
	for scanner.Scan() {
		line := scanner.Text()
		result += line
		if strings.Contains(line, "\"") {
			break
		}
	}
	return extractQuotedValue(result)
}

func extractAdresses(astr string) int {
	cleaned := strings.Map(func(r rune) rune {
		if unicode.IsDigit(r) || r == ',' || r == ' ' {
			return r
		}
		return -1
	}, astr)

	parts := strings.Split(cleaned, ",")
	for i, part := range parts {
		if i >= MAXNUMADR {
			break
		}
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if val, err := strconv.ParseUint(part, 10, 8); err == nil {
			scanAddress[numAdresses] = byte(val)
			numAdresses++
		}
	}
	return numAdresses
}

func OpenPort(devStr string) (*SerialPort, error) {
	config := &serial.Config{
		Name:        devStr,
		Baud:        19200,
		Size:        8,
		Parity:      serial.ParityNone,
		StopBits:    serial.Stop1,
		ReadTimeout: 100 * time.Millisecond,
	}

	port, err := serial.OpenPort(config)
	if err != nil {
		return nil, fmt.Errorf("failed to open port %s: %w", devStr, err)
	}

	return &SerialPort{port: port}, nil
}

func (sp *SerialPort) WriteStrPort(chars string, adr byte) error {
	var txbuff [TXBUFFLEN]byte
	var bcc byte
	a := 0

	// Initialize buffer (not strictly needed in Go as arrays zero-initialize)
	for x := 0; x < TXBUFFLEN; x++ {
		txbuff[x] = 0x00
	}

	// ADR+0x80
	bcc = 0x00
	txbuff[a] = adr + 0x80
	//bcc ^= txbuff[a]
	a++

	for i := 0; i < len(chars); i++ {
		if a >= TXBUFFLEN-2 { // Leave space for ETX and BCC
			return fmt.Errorf("message exceeds buffer size")
		}
		txbuff[a] = chars[i]
		bcc ^= txbuff[a]
		a++
	}

	// ETX
	if a >= TXBUFFLEN-1 {
		return fmt.Errorf("message too long for ETX")
	}
	txbuff[a] = ETX
	bcc ^= txbuff[a]
	a++

	// BCC
	if a >= TXBUFFLEN {
		return fmt.Errorf("message too long for BCC")
	}
	txbuff[a] = bcc
	a++

	// Write to serial port
	n, err := sp.port.Write(txbuff[:a])
	if err != nil {
		slog.Debug("write failed");
		return fmt.Errorf("write failed: %w", err)
	}
	if n != a {
		slog.Debug("incomplete write", "expected", a, "wrote", n)
		return fmt.Errorf("incomplete write, expected %d, wrote %d", a, n)
	}

	return nil
}

func (sp *SerialPort) ReadStrPort() (byte, string, error) {
	result := make([]byte, RXBUFFLEN)

	// Read with timeout is handled by the serial port config
	iIn, err := sp.port.Read(result)
	if err != nil {
		if os.IsTimeout(err) {
			return 0x00, "", fmt.Errorf("read timeout: %w", err)
		}
		return 0x00, "", fmt.Errorf("serial read error: %w", err)
	}

	if iIn <= 0 {
		return 0x00, "", errors.New("no data read")
	}

	// Checksum calculation (BCC)
	bcc := byte(0x00)
	for n := 0; n < iIn-1; n++ {
		bcc ^= result[n]
	}

	// Verify BCC
	if bcc != result[iIn-1] {
		return 0x00, "", errors.New("BCC verification failed")
	}

	// Replace BCC with string terminator
	result[iIn-1] = 0x00

	// Return first byte of result (address)
	return result[0], "", nil
}

func (sp *SerialPort) Close() error {
	if sp.port != nil {
		return sp.port.Close()
	}
	return nil
}

func getSerialNumber() error {
	serNoStr[adrCounter] = ""
	cmd := "SN ?"
	var portStatus int
	var err error

	retryCnt[adrCounter] = 0
	for ; retryCnt[adrCounter] < maxRetrys; retryCnt[adrCounter]++ {
		portStatus, err = getValue(&serNoStr[adrCounter], cmd, scanAddress[adrCounter])
		if err == nil && portStatus >= 0 {
			if showValues {
				slog.Debug("getSerialNumber", "Serialnumber", serNoStr[adrCounter])
			}
			break
		} else if portStatus == NAK {
			msgNAK[adrCounter]++
			if showValues {
				slog.Debug("NAK received", "sent", msgSent[adrCounter], 
					"received", msgReceived[adrCounter], "NAK", msgNAK[adrCounter])
			}
			continue
		} else if showValues {
			slog.Error("SN Error")
		}
	}
	return err
}

func getMeasurement() error {
	cmd := "MEA CH 1 ?"
	var portStatus int
	var err error

	if _, _, err := serialPort.ReadStrPort(); err != nil && showValues {
		slog.Error("Dummy read error:", "error", err)
	}

	for ; retryCnt[adrCounter] < maxRetrys; retryCnt[adrCounter]++ {
		portStatus, err = getValue(&valueStr[adrCounter], cmd, scanAddress[adrCounter])
		if err == nil && portStatus == ACK {
			if showValues {
				slog.Debug("Measurement", "SN", serNoStr[adrCounter], "Theta", valueStr[adrCounter], 
					"TX", msgSent[adrCounter], "RX", msgReceived[adrCounter], "NAK", msgNAK[adrCounter])
			}
			timestamp[adrCounter] = time.Now()
			break
		} else if portStatus == NAK {
			msgNAK[adrCounter]++
			continue
		}
	}
	return err
}

func getValue(resultStr *string, cmdStr string, adr byte) (int, error) {
	if showValues {
		slog.Debug("getValue", "cmdStr", cmdStr, "adr", adr, "port", fmt.Sprintf("%v", serialPort))
	}

    *resultStr = ""

	if err := serialPort.WriteStrPort(cmdStr, adr); err != nil {
		if showValues {
			slog.Error("write failed:", "error", err)
		}
		return 0, err
	}

	msgSent[adr]++
	time.Sleep(485 * time.Millisecond)

	readChar, bufStr, err := serialPort.ReadStrPort()
	if err != nil {
		if showValues {
			slog.Debug("read failed: error", "error", err)
		}
		return 0, err
	}

	msgReceived[adr]++

	// Convert string to []byte for ETX processing
    buf := []byte(bufStr)

	// Find ETX and truncate
    if etxPos := bytes.IndexByte(buf, ETX); etxPos != -1 {
        buf = buf[:etxPos]
    }

    // Filter non-printable characters
    var result bytes.Buffer
    for i := 0; i < len(buf); i++ {
        if buf[i] == ETX {
            break
        }
        r := rune(buf[i])
        if unicode.IsPrint(r) || unicode.IsSpace(r) || buf[i] == 0 {
            result.WriteByte(buf[i])
        }
    }

    *resultStr = result.String()
    return int(readChar), nil
}

func writeToDB(serNoStr, valueStr string, t time.Time) int {// Connect to database
	// Connect to database
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", db.User, db.Passwd, db.Host, db.Name)
	sock, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Errorf("database connection failed: %v", err)
		slog.Debug("database connection failed", "dsn", dsn)
		return 1
	}
	defer sock.Close()

	// Verify connection
	if err = sock.Ping(); err != nil {
		fmt.Errorf("database ping failed: %v", err)
		slog.Debug("database ping failed", "dsn", dsn)
		return 1
	}


	// Get channel ID
	var idChannel int
	query := "SELECT channel.id FROM channel LEFT JOIN unit ON channel.id_unit = unit.id WHERE unit.serialnumber = ?"
	row := sock.QueryRow(query, serNoStr)
	if err := row.Scan(&idChannel); err != nil {
		if err == sql.ErrNoRows {
			return 3
		}
		return 2
	}

	// Prepare to write data
	var qbuf string
	if strings.HasPrefix(valueStr, "100003") || strings.HasPrefix(valueStr, "100002") || strings.HasPrefix(valueStr, "100001") {
		qbuf = fmt.Sprintf("UPDATE `channel` SET `status`='%s' WHERE `id`='%d'", valueStr, idChannel)
	} else {
		// Write status
		qbuf = fmt.Sprintf("UPDATE `channel` SET `status`='%s' WHERE `id`='%d'", "normal", idChannel)
		if _, err := sock.Exec(qbuf); err != nil {
			return 4
		}

		// Prepare data insert
		qbuf = fmt.Sprintf("INSERT INTO `data` (`id_channel`,`datetime`,`value`) VALUES ('%d','%s','%s')", 
			idChannel, makeDatetime(t), valueStr)
	}

	// Execute the final query
	if _, err := sock.Exec(qbuf); err != nil {
		return 5
	}

	return 0
}

func writeToPostgres(serNoStr, valueStr string, t time.Time) int {
    // Connect to database
    dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", 
        db.Host, db.User, db.Passwd, db.Name)
    sock, err := sql.Open("postgres", dsn)
    if err != nil {
        fmt.Errorf("database connection failed: %v", err)
        slog.Debug("database connection failed", "dsn", dsn)
        return 1
    }
    defer sock.Close()

    // Verify connection
    if err = sock.Ping(); err != nil {
        fmt.Errorf("database ping failed: %v", err)
        slog.Debug("database ping failed", "dsn", dsn)
        return 1
    }

    // Get channel ID
    var idChannel int
    query := "SELECT channel.id FROM channel LEFT JOIN unit ON channel.id_unit = unit.id WHERE unit.serialnumber = $1"
    row := sock.QueryRow(query, serNoStr)
    if err := row.Scan(&idChannel); err != nil {
        if err == sql.ErrNoRows {
			slog.Debug("DB", "query", query, "serNoStr", serNoStr);
            return 3
        }
        return 2
    }

    // Prepare to write data
    var qbuf string
    if strings.HasPrefix(valueStr, "100003") || strings.HasPrefix(valueStr, "100002") || strings.HasPrefix(valueStr, "100001") {
        qbuf = fmt.Sprintf("UPDATE channel SET status='%s' WHERE id='%d'", valueStr, idChannel)
    } else {
        // Write status
        qbuf = fmt.Sprintf("UPDATE channel SET status='%s' WHERE id='%d'", "normal", idChannel)
        if _, err := sock.Exec(qbuf); err != nil {
            return 4
        }

        // Prepare data insert
        qbuf = fmt.Sprintf("INSERT INTO data (id_channel, datetime, value) VALUES ('%d','%s','%s')", 
            idChannel, makeDatetime(t), valueStr)
    }

    // Execute the final query
    if _, err := sock.Exec(qbuf); err != nil {
		slog.Debug("DB", "query", qbuf);
        return 5
    }

    return 0
}

// You need to implement this function if it's missing
func makeDatetime(t time.Time) string {
    return t.Format("2006-01-02 15:04:05") // MySQL datetime format
}


func cleanup() {
	if serialPort != nil {
		serialPort.Close()
	}
	os.Remove(LOCK_FILE)
}
