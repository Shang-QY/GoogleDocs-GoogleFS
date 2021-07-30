package greeting

import (
	"awesomeGFS/gfs/util"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

type Action struct {
	T time.Time
	Type int
	Serial int
	Path1 string
	Path2 string
}

// Hello returns a greeting for the named person.
func Hello(name string) string {
	// Return a greeting that embeds the name in a message.
	message := fmt.Sprintf("Hi, %v. Welcome!", name)
	path := "test.log"
	reOpen(path)
	//path := "gfs/log/log.txt"
	//LogInfo(path)
	//parseLog(path)
	return message
}

func LogInfo(path string) {
	// open log file
	logFile, err := os.OpenFile(path, os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {panic(err)}
	defer logFile.Close()
	// init logger
	//logger := log.New()
	//logger.SetOutput(logFile)
	//logger.SetFormatter(&log.JSONFormatter{})
	//logger.Print("use logger")
	// actions
	action1 := Action{T: time.Now(), Type: util.MKDIR, Serial: 1, Path1: "/usr"}
	action2 := Action{T: time.Now(), Type: util.CREATE, Serial: 2, Path1: "/usr/1.txt"}
	action3 := Action{T: time.Now(), Type: util.COMMIT, Serial: 1}
	// log into log files
	marshal1, err := json.Marshal(action1)
	println(string(marshal1))
	_, err = logFile.Write(append(marshal1, '\n'))
	if err != nil {panic(err)}

	marshal2, err := json.Marshal(action2)
	println(string(marshal2))
	_, err = logFile.Write(append(marshal2, '\n'))
	if err != nil {panic(err)}

	marshal3, err := json.Marshal(action3)
	println(string(marshal3))
	_, err = logFile.Write(append(marshal3, '\n'))
	if err != nil {panic(err)}
}

func parseLog(path string) {
	var rd *bufio.Reader
	var line string
	var action Action

	logFile, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {panic(err)}
	defer logFile.Close()

	rd = bufio.NewReader(logFile)
	for ;; {
		line, err = rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		//log.Println(line)

		// use json parser
		//value, _ := jsonparser.GetString([]byte(line), "level")
		//println("jsonparser: ", value)

		// use json unmarshal
		err := json.Unmarshal([]byte(line), &action)
		if err != nil {println(err)}
		println("Unmarshal", action.Type, action.Serial, action.Path1)
	}

	// use strconv to parse
	//path1 := "/ab/cd"
	//path2 := "/12/34"
	//// parse
	//logLine := fmt.Sprint(12, " ", path1, " ", path2)
	////println(logLine)
	//parseInt, err := strconv.ParseInt(logLine, 10, 32)
	//println(parseInt, err)
}

func reOpen(path string) {
	//var logFile *os.File
	logFile, err := os.OpenFile(path, os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {panic(err)}

	_, err = logFile.Write([]byte("xxx\n"))
	if err != nil {panic(err)}

	println(logFile)
	err = logFile.Close()
	if err != nil {panic(err)}
	println(logFile)

	logFile, err = os.OpenFile(path, os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {panic(err)}
	println(logFile)
	rd := bufio.NewReader(logFile)
	for ;; {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {break}
		println(line)
	}

}

