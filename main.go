package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sirupsen/logrus"
)

const Version = "1.0.1"

type (
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     *logrus.Logger
	}
)

type Options struct {
	Logger *logrus.Logger
}

func NewConsoleLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.DebugLevel)
	return logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}
	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = NewConsoleLogger()
	}

	driver := &Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debugf("Using %s (database already exists)\n", dir)
		return driver, nil
	}

	opts.Logger.Debugf("Creating the database at %s ...\n", dir)
	return driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Read(collection string, resource string, v string) error {

	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}

	b, err := os.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func (d *Driver) Delete(collection, resource string) error {

	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file or directory named %v\n", path)

	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}
	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}

	return fi, err
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode string
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, fnlPath); err != nil {
		return err
	}

	d.log.Debugf("Successfully wrote %s/%s", collection, resource)
	return nil
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing collection - unable to read")
	}
	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var records []string

	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	d.log.Debugf("Successfully read all records from %s", collection)
	return records, nil
}

func main() {
	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	employees := []User{
		{"Smoker", "23", "9234923492", "Surya Tech", Address{"logue town", "Kinki", "Japan", "008"}},
		{"Zoro", "23", "9234923492", "Asura Tech", Address{"Shimotsuki Village", "East Blue", "Mars", "008"}},
		{"Benn", "23", "9234923492", "Yantra Tech", Address{"Shanks' Ship", "Grand Line", "Nepal", "008"}},
		{"Doflamingo", "23", "9234923492", "String Tech", Address{"Dressrosa", "New World", "Australia", "008"}},
		{"Sabo", "23", "9234923492", "Agni Tech", Address{"Baltigo", "Grand Line", "Equador", "008"}},
		{"Kuma", "23", "9234923492", "Panda Tech", Address{"Sorbet Kingdom", "South Blue", "South Africa", "008"}},
		{"Kid", "23", "9234923492", "Montessori Tech", Address{"South Blue", "South Blue", "Argentina", "008"}},
	}

	for _, value := range employees {
		err := db.Write("users", value.Name, value)
		if err != nil {
			fmt.Println("Error writing record:", err)
		}
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error reading records:", err)
	}

	fmt.Println("Records", records)

	allusers := []User{}

	for _, f := range records {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println("Error", err)
		}
		allusers = append(allusers, employeeFound)
	}
	fmt.Println(allusers)
}
