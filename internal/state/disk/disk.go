package disk

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/vpol/ometria/internal/state"
	"log"
	"os"
	"sync"
)

const lockName = ".lock"

var (
	path     string
	filename string
	logger   *log.Logger
)

func init() {
	flag.StringVar(&path, "disk_filepath", "../../data", "file path (without trailing slash)")
	flag.StringVar(&filename, "disk_fileprefix", "prefix", "file prefix")
	logger = log.New(os.Stdout, "disk ", log.LstdFlags)
}

type StorageImpl struct {
	lockFile *os.File
	mu       sync.Mutex
}

func New() *StorageImpl {

	if path == "" {
		logger.Panic("file path is incorrect")
	}

	lockFilePath := fmt.Sprintf("%s/%s", path, lockName)

	_, err := os.OpenFile(lockFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)

	if err != nil {
		logger.Panicf("directory is not accessible, %s", err.Error())
	}

	err = os.Remove(lockFilePath)

	if err != nil {
		logger.Panicf("directory is not accessible, %s", err.Error())
	}

	return &StorageImpl{
		mu: sync.Mutex{},
	}
}

func (f *StorageImpl) Get(id string) (state.State, error) {

	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := fmt.Sprintf("%s/%s", path, id)

	file, err := os.Open(filePath)

	if err != nil {
		if os.IsNotExist(err) {
			return state.State{LastUpdate: 0}, nil
		}

		return state.State{}, err
	}

	defer file.Close()

	d := json.NewDecoder(file)

	var st state.State

	if err := d.Decode(&st); err != nil {
		return state.State{}, err
	}

	return st, nil
}

func (f *StorageImpl) Update(id string, data state.State) (err error) {

	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := fmt.Sprintf("%s/%s", path, id)

	var file *os.File

	file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		return err
	}

	defer file.Close()

	d := json.NewEncoder(file)

	if err = d.Encode(data); err != nil {
		return
	}

	return
}
