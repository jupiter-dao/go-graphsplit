package graphsplit

import (
	"fmt"
	"os"
)

const Gib = 1024 * 1024 * 1024

type ExtraFile struct {
	path         string
	files        []Finfo
	idx          int
	sliceSize    int64
	pieceRawSize int64
}

func NewRealFile(path string, sliceSize int64, pieceRawSize int64, randomRenameSourceFile bool) (*ExtraFile, error) {
	rf := &ExtraFile{path: path, sliceSize: sliceSize, pieceRawSize: pieceRawSize}
	if path != "" {
		finfo, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !finfo.IsDir() {
			return nil, fmt.Errorf("the path %s is not a directory", path)
		}
		rf.walk(randomRenameSourceFile)
	}

	return rf, nil
}

func (rf *ExtraFile) walk(randomRenameSourceFile bool) {
	files := GetFileListAsync([]string{rf.path})
	for item := range files {
		rf.files = append(rf.files, item)
	}
	if randomRenameSourceFile {
		rf.files = tryRenameFileName(rf.files)
	}
	Shuffle(rf.files)
}

func (rf *ExtraFile) getFiles() []Finfo {
	count := len(rf.files)
	if count == 0 {
		return nil
	}
	var total int64
	var files []Finfo
	startIdx := rf.idx
	for total < rf.sliceSize {
		file := rf.files[rf.idx]
		if total+file.Info.Size()+rf.pieceRawSize <= 32*Gib {
			total += file.Info.Size()
			files = append(files, file)
		}
		rf.idx = (rf.idx + 1) % count

		if rf.idx == startIdx {
			break
		}
	}

	return files
}
