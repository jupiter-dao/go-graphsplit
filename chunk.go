package graphsplit

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("graphsplit")

type GraphBuildCallback interface {
	OnSuccess(buf *Buffer, graphName, payloadCid, fsDetail string)
	OnError(error)
}

type commPCallback struct {
	carDir     string
	rename     bool
	addPadding bool
}

func (cc *commPCallback) OnSuccess(buf *Buffer, graphName, payloadCid, fsDetail string) {
	commpStartTime := time.Now()

	log.Info("start to calculate pieceCID")
	cpRes, err := CalcCommPV2(buf, cc.addPadding)
	if err != nil {
		log.Fatalf("calculation of pieceCID failed: %s", err)
	}
	log.Infof("calculation of pieceCID completed, time elapsed: %s", time.Since(commpStartTime))
	log.Infof("piece cid: %s, payload size: %d, size: %d ", cpRes.Root.String(), cpRes.PayloadSize, cpRes.Size)

	buf.SeekStart()
	carFilePath := filepath.Join(cc.carDir, cpRes.Root.String())
	carFileNameWithSuffix := carFilePath + ".car"

	carFile, err := os.OpenFile(carFileNameWithSuffix, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		log.Fatalf("failed to create car file: %s", err)
	}

	if _, err = io.Copy(carFile, buf); err != nil {
		log.Fatalf("failed to write car file: %s", err)
	}
	buf.Reset()
	carFile.Close()

	if cc.rename {
		if err := os.Rename(carFileNameWithSuffix, carFilePath); err != nil {
			log.Fatalf("failed to rename car file: %s", err)
		}
	}

	// Add node inof to manifest.csv
	manifestPath := path.Join(cc.carDir, "manifest.csv")
	_, err = os.Stat(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	var isCreateAction bool
	if err != nil && os.IsNotExist(err) {
		isCreateAction = true
	}
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	csvWriter := csv.NewWriter(f)
	csvWriter.UseCRLF = true
	defer csvWriter.Flush()
	if isCreateAction {
		csvWriter.Write([]string{
			"payload_cid", "filename", "piece_cid", "payload_size", "piece_size", "detail",
		})
	}

	if err := csvWriter.Write([]string{
		payloadCid, graphName, cpRes.Root.String(),
		strconv.FormatInt(cpRes.PayloadSize, 10), strconv.FormatUint(uint64(cpRes.Size), 10), fsDetail,
	}); err != nil {
		log.Fatal(err)
	}
}

func (cc *commPCallback) OnError(err error) {
	log.Fatal(err)
}

type csvCallback struct {
	carDir string
}

func (cc *csvCallback) OnSuccess(buf *Buffer, graphName, payloadCid, fsDetail string) {
	// Add node inof to manifest.csv
	manifestPath := path.Join(cc.carDir, "manifest.csv")
	_, err := os.Stat(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	var isCreateAction bool
	if err != nil && os.IsNotExist(err) {
		isCreateAction = true
	}
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if isCreateAction {
		if _, err := f.Write([]byte("payload_cid,filename,detail")); err != nil {
			log.Fatal(err)
		}
	}

	if err := os.WriteFile(path.Join(cc.carDir, payloadCid+".car"), buf.Bytes(), 0o644); err != nil {
		log.Fatal(err)
	}

	if _, err := f.Write([]byte(fmt.Sprintf("%s,%s,%s", payloadCid, graphName, fsDetail))); err != nil {
		log.Fatal(err)
	}
}

func (cc *csvCallback) OnError(err error) {
	log.Fatal(err)
}

type errCallback struct{}

func (cc *errCallback) OnSuccess(*Buffer, string, string, string) {}
func (cc *errCallback) OnError(err error) {
	log.Fatal(err)
}

func CommPCallback(carDir string, rename, addPadding bool) GraphBuildCallback {
	return &commPCallback{carDir: carDir, rename: rename, addPadding: addPadding}
}

func CSVCallback(carDir string) GraphBuildCallback {
	return &csvCallback{carDir: carDir}
}

func ErrCallback() GraphBuildCallback {
	return &errCallback{}
}

type ChunkParams struct {
	ExpectSliceSize        int64
	ParentPath             string
	TargetPath             string
	CarDir                 string
	GraphName              string
	Parallel               int
	Cb                     GraphBuildCallback
	Ef                     *ExtraFile
	RandomRenameSourceFile bool
	RandomSelectFile       bool
	SkipFilename           bool
}

func Chunk(ctx context.Context, params *ChunkParams) error {
	var cumuSize int64 = 0
	graphSliceCount := 0
	graphFiles := make([]Finfo, 0)
	if params.ExpectSliceSize == 0 {
		return fmt.Errorf("slice size has been set as 0")
	}
	if params.Parallel <= 0 {
		return fmt.Errorf("parallel has to be greater than 0")
	}
	if params.ParentPath == "" {
		params.ParentPath = params.TargetPath
	}

	partSliceSize := params.ExpectSliceSize - params.Ef.sliceSize
	args := []string{params.TargetPath}
	sliceTotal := GetGraphCount(args, params.ExpectSliceSize)
	if sliceTotal == 0 {
		log.Warn("Empty folder or file!")
		return nil
	}
	var allFiles []Finfo
	files := GetFileListAsync(args)
	for item := range files {
		allFiles = append(allFiles, item)
	}
	log.Infof("total files: %d", len(allFiles))

	Shuffle(allFiles)

	for _, item := range allFiles {
		item := item
		if params.RandomRenameSourceFile {
			item = tryRenameFileName([]Finfo{item})[0]
		}
		// log.Infof("name: %s", item.Name)
		fileSize := item.Info.Size()
		switch {
		case cumuSize+fileSize < partSliceSize:
			cumuSize += fileSize
			graphFiles = append(graphFiles, item)
		case cumuSize+fileSize == partSliceSize:
			cumuSize += fileSize
			graphFiles = append(graphFiles, item)
			// todo build ipld from graphFiles
			BuildIpldGraph(ctx, append(params.Ef.getFiles(), graphFiles...), GenGraphName(params.GraphName, graphSliceCount, sliceTotal), params)
			log.Infof("cumu-size: %d", cumuSize)
			log.Infof("%s", GenGraphName(params.GraphName, graphSliceCount, sliceTotal))
			log.Infof("=================")
			cumuSize = 0
			graphFiles = make([]Finfo, 0)
			graphSliceCount++
		case cumuSize+fileSize > partSliceSize:
			fileSliceCount := 0
			// need to split item to fit graph slice
			//
			// first cut
			firstCut := partSliceSize - cumuSize
			var seekStart int64 = 0
			var seekEnd int64 = seekStart + firstCut - 1
			log.Infof("first cut %d, seek start at %d, end at %d", firstCut, seekStart, seekEnd)
			log.Infof("----------------")
			fi := Finfo{
				Path:      item.Path,
				Name:      fmt.Sprintf("%s.%08d", item.Info.Name(), fileSliceCount),
				Info:      item.Info,
				SeekStart: seekStart,
				SeekEnd:   seekEnd,
			}
			if params.RandomRenameSourceFile {
				graphFiles = append(graphFiles, tryRenameFileName([]Finfo{fi})...)
			} else {
				graphFiles = append(graphFiles, fi)
			}
			fileSliceCount++
			// todo build ipld from graphFiles
			BuildIpldGraph(ctx, append(params.Ef.getFiles(), graphFiles...), GenGraphName(params.GraphName, graphSliceCount, sliceTotal), params)
			log.Infof("cumu-size: %d", cumuSize+firstCut)
			log.Infof("%s", GenGraphName(params.GraphName, graphSliceCount, sliceTotal))
			log.Infof("=================")
			cumuSize = 0
			graphFiles = make([]Finfo, 0)
			graphSliceCount++
			for seekEnd < fileSize-1 {
				seekStart = seekEnd + 1
				seekEnd = seekStart + partSliceSize - 1
				if seekEnd >= fileSize-1 {
					seekEnd = fileSize - 1
				}
				log.Infof("following cut %d, seek start at %d, end at %d", seekEnd-seekStart+1, seekStart, seekEnd)
				log.Infof("----------------")
				cumuSize += seekEnd - seekStart + 1
				fi := Finfo{
					Path:      item.Path,
					Name:      fmt.Sprintf("%s.%08d", item.Info.Name(), fileSliceCount),
					Info:      item.Info,
					SeekStart: seekStart,
					SeekEnd:   seekEnd,
				}
				if params.RandomRenameSourceFile {
					graphFiles = append(graphFiles, tryRenameFileName([]Finfo{fi})...)
				} else {
					graphFiles = append(graphFiles, fi)
				}

				fileSliceCount++
				if seekEnd-seekStart == partSliceSize-1 {
					// todo build ipld from graphFiles
					BuildIpldGraph(ctx, append(params.Ef.getFiles(), graphFiles...), GenGraphName(params.GraphName, graphSliceCount, sliceTotal), params)
					log.Infof("cumu-size: %d", partSliceSize)
					log.Infof("%s", GenGraphName(params.GraphName, graphSliceCount, sliceTotal))
					log.Infof("=================")
					cumuSize = 0
					graphFiles = make([]Finfo, 0)
					graphSliceCount++
				}
			}
		}
	}
	if cumuSize > 0 {
		// todo build ipld from graphFiles
		BuildIpldGraph(ctx, append(params.Ef.getFiles(), graphFiles...), GenGraphName(params.GraphName, graphSliceCount, sliceTotal), params)
		log.Infof("cumu-size: %d", cumuSize)
		log.Infof("%s", GenGraphName(params.GraphName, graphSliceCount, sliceTotal))
		log.Infof("=================")
	}
	return nil
}
