package graphsplit

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
)

// GetVideoDuration 返回视频总时长（秒）
func GetVideoDuration(videoPath string) (string, error) {
	// Validate input
	if videoPath == "" {
		return "", fmt.Errorf("videoPath cannot be empty")
	}

	// Check if the video file exists and is readable
	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		return "", fmt.Errorf("video file does not exist: %s", videoPath)
	} else if err != nil {
		return "", fmt.Errorf("failed to access video file %s: %w", videoPath, err)
	}

	// Construct the ffprobe command
	cmd := exec.Command("ffprobe", "-v", "error", "-show_entries",
		"format=duration", "-of", "default=noprint_wrappers=1:nokey=1", videoPath)

	// Capture both stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run the command
	err := cmd.Run()
	if err != nil {
		// Include stderr in the error message for debugging
		return "", fmt.Errorf("ffprobe failed for %s: %w, stderr: %s", videoPath, err, stderr.String())
	}

	// Trim whitespace from the output
	durationStr := strings.TrimSpace(stdout.String())
	if durationStr == "" {
		return "", fmt.Errorf("no duration found for %s", videoPath)
	}

	return durationStr, nil
}

// FormatSecondsToHHMMSS 将秒数转 hh:mm:ss.xxx 字符串
func FormatSecondsToHHMMSS(sec float64) string {
	h := int(sec) / 3600
	m := (int(sec) % 3600) / 60
	s := sec - float64(h*3600+m*60)
	return fmt.Sprintf("%02d:%02d:%06.3f", h, m, s)
}

// VideoSlice 调用 ffmpeg 生成视频片段（基于编号）
func (rf *VideoFile) VideoSlice(startTime string, index int64) (string, error) {
	// Validate input parameters
	if rf.videoSourcePath == "" {
		return "", fmt.Errorf("videoSourcePath cannot be empty")
	}
	if rf.videoOutputPath == "" {
		return "", fmt.Errorf("videoOutputPath cannot be empty")
	}
	if startTime == "" {
		return "", fmt.Errorf("startTime cannot be empty")
	}
	if rf.endTime == "" {
		return "", fmt.Errorf("endTime cannot be empty")
	}

	// Check if the source video file exists and is readable
	if _, err := os.Stat(rf.videoSourcePath); os.IsNotExist(err) {
		return "", fmt.Errorf("source video file does not exist: %s", rf.videoSourcePath)
	} else if err != nil {
		return "", fmt.Errorf("failed to access source video file %s: %w", rf.videoSourcePath, err)
	}

	// Ensure output directory exists
	if err := os.MkdirAll(rf.videoOutputPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory %s: %w", rf.videoOutputPath, err)
	}

	// Generate output file path
	countName := strconv.Itoa(int(index))
	outputPath := path.Join(rf.videoOutputPath, rf.baseRename+countName+".mp4")

	// Construct ffmpeg command
	args := []string{
		"ffmpeg",
		"-ss", startTime,
		"-i", rf.videoSourcePath,
		"-t", rf.endTime, // Use -t for duration; change to -to if endTime is a timestamp
		"-c", "copy",
		outputPath,
		"-y",
	}
	cmd := exec.Command(args[0], args[1:]...)

	// Capture stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run the command
	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("ffmpeg slice failed for %s: %w, stderr: %s", rf.videoSourcePath, err, stderr.String())
	}

	// Verify output file was created
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return "", fmt.Errorf("output file was not created: %s", outputPath)
	} else if err != nil {
		return "", fmt.Errorf("failed to verify output file %s: %w", outputPath, err)
	}

	// Optionally update files slice or counter (uncomment if needed)
	// rf.files = append(rf.files, Finfo{Name: outputPath})
	// atomic.AddInt64(&rf.counter, 1)

	return outputPath, nil
}

type VideoFile struct {
	videoSourcePath string
	videoOutputPath string
	files           []Finfo
	endTime         string // 计算一次
	counter         int64  // 视频使用次数，原子计数
	baseRename      string
	sliceSize       int64
}

func NewVideoFile(videoSourcePath string, videoOutputPath string, countOld int64, baseRename string) (*VideoFile, error) {
	if videoSourcePath != "" {
		_, err := os.Stat(videoSourcePath)
		if err != nil {
			return nil, err
		}
		log.Infof("use seed file video: %s", videoSourcePath)

		// 计算video的时间
		duration, err := GetVideoDuration(videoSourcePath)
		if err != nil {
			return nil, err
		}
		rf := &VideoFile{
			videoSourcePath: videoSourcePath,
			videoOutputPath: videoOutputPath,
			counter:         countOld,
			endTime:         duration,
			baseRename:      baseRename,
		}
		log.Infof("videoSourcePath %+v,videoOutputPath:%+v ", rf.videoSourcePath, rf.videoOutputPath)
		return rf, nil
	}
	return nil, fmt.Errorf("videoSourcePath is null")
}

// getFiles 并发安全地生成文件切片
func (rf *VideoFile) getFiles() []Finfo {
	var files []Finfo

	// 原子自增，并返回新值
	current := atomic.AddInt64(&rf.counter, 1)
	index := current - 1 // 从0开始
	log.Infof("video index %+v", index)
	// 根据计数去切割视频
	beginTime := timeDelayMS(int(index))
	filename, err := rf.VideoSlice(beginTime, index)
	if err != nil {
		log.Errorf("split video err %s ", err.Error())
		return files
	}
	stat, err := os.Stat(filename)
	if err != nil {
		return nil
	}

	files = append(files, Finfo{
		Path:      filename,
		Name:      stat.Name(),
		Info:      stat,
		SeekStart: 0,
		SeekEnd:   stat.Size(),
	})
	return files
}

func timeDelayMS(ms int) string {
	sec := ms / 1000
	h := strconv.Itoa(sec / 3600)
	m := strconv.Itoa(sec % 3600 / 60)
	s := strconv.Itoa(sec % 3600 % 60)
	s1 := strconv.Itoa(ms % 1000)
	r := h + ":" + m + ":" + s + "." + s1
	return r
}
