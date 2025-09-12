package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"os"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/filedrive-team/go-graphsplit"
	"github.com/filedrive-team/go-graphsplit/config"
	"github.com/filedrive-team/go-graphsplit/dataset"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("graphsplit")

func main() {
	logging.SetLogLevel("*", "INFO")
	local := []*cli.Command{
		chunkCmd,
		restoreCmd,
		commpCmd,
		importDatasetCmd,
	}

	app := &cli.App{
		Name:     "graphsplit",
		Commands: local,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}

var chunkCmd = &cli.Command{
	Name:  "chunk",
	Usage: "Generate CAR files of the specified size",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "parallel",
			Value: 2,
			Usage: "specify how many number of goroutines runs when generate file node",
		},
		&cli.StringFlag{
			Name:     "graph-name",
			Required: true,
			Usage:    "specify graph name",
		},
		&cli.StringFlag{
			Name:     "car-dir",
			Required: true,
			Usage:    "specify output CAR directory",
		},
		&cli.StringFlag{
			Name:  "parent-path",
			Value: "",
			Usage: "specify graph parent path",
		},
		&cli.BoolFlag{
			Name:  "save-manifest",
			Value: true,
			Usage: "create a mainfest.csv in car-dir to save mapping of data-cids and slice names",
		},
		&cli.BoolFlag{
			Name:  "calc-commp",
			Value: true,
			Usage: "create a mainfest.csv in car-dir to save mapping of data-cids, slice names, piece-cids and piece-sizes",
		},
		&cli.BoolFlag{
			Name:  "rename",
			Value: false,
			Usage: "rename carfile to piece",
		},
		&cli.BoolFlag{
			Name:  "random-rename-source-file",
			Value: false,
			Usage: "random rename source file name",
		},
		&cli.BoolFlag{
			Name:  "add-padding",
			Value: false,
			Usage: "add padding to carfile in order to convert it to piece file",
		},
		&cli.StringFlag{
			Name:    "config",
			Usage:   "config file path",
			Aliases: []string{"c"},
		},
		&cli.BoolFlag{
			Name:  "loop",
			Usage: "loop chunking",
		},
		&cli.BoolFlag{
			Name:  "random-select-file",
			Usage: "random select file to chunk",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "skip-filename",
			Usage: "manifest csv detail not contain filename",
			Value: true,
		},
		&cli.IntFlag{
			Name:  "base-limit",
			Usage: "base of limit",
			Value: 0,
		},
		&cli.StringFlag{
			Name:  "base-rename",
			Usage: "base of rename",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "video-path",
			Usage: "original video path",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "video-output-path",
			Usage: "video output path",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "limit",
			Usage: "number of limit",
			Value: 1000,
		},
		&cli.BoolFlag{
			Name:  "send",
			Usage: "is send deal",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "providers",
			Usage: "storage provider on-chain address(multiple are separated by -)",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "duration",
			Usage: "duration of the deal in epochs",
			Value: 1036800, // default is 2880 * 460 == 1036800 days
		},
		&cli.Int64Flag{
			Name:  "wait",
			Usage: "wait send",
			Value: 0,
		},
		&cli.BoolFlag{
			Name:  "delete-after-import",
			Usage: "whether to delete the data for the offline deal after the deal has been added to a sector",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "remove-unsealed-copy",
			Usage: "indicates that an unsealed copy of the sector in not required for fast retrieval",
			Value: false,
		},
		&cli.StringFlag{
			Name:    "dsn",
			Aliases: []string{"d"},
			Usage:   "input the dsn address",
		},
	},
	ArgsUsage: "<input path>",
	Action: func(c *cli.Context) error {
		ctx := context.Background()
		parallel := c.Uint("parallel")
		parentPath := c.String("parent-path")
		carDir := c.String("car-dir")
		graphName := c.String("graph-name")
		randomRenameSourceFile := c.Bool("random-rename-source-file")
		randomSelectFile := c.Bool("random-select-file")
		skipFilename := c.Bool("skip-filename")
		if !graphsplit.ExistDir(carDir) {
			return fmt.Errorf("the path of car-dir does not exist")
		}
		baseRename := c.String("base-rename")
		if baseRename == "" {
			baseRename = uuid.New().String()
		}
		videoPath := c.String("video-path")
		if videoPath == "" {
			return fmt.Errorf("must input video path")
		}
		limitN := c.Int("limit")
		if limitN <= 0 {
			return fmt.Errorf("limit cannot be less than 0")
		}
		endTime, err := graphsplit.GetVideoDuration(videoPath)
		if err != nil {
			return fmt.Errorf("get video duration err %s", err.Error())
		}
		log.Infof("videoPath %s ,duration : %+v s", videoPath, endTime)
		baseLimit := c.Int("base-limit")
		cfgPath := c.String("config")
		if cfgPath == "" {
			return fmt.Errorf("config file path is required")
		}

		cfg, err := config.LoadConfig(cfgPath)
		if err != nil {
			return fmt.Errorf("failed to load config file(%s): %v", cfgPath, err)
		}
		log.Infof("config file: %+v", cfg)

		log.Infof("old slice size: %d", cfg.SliceSize)
		cfg.SliceSize++
		sliceSize := cfg.SliceSize
		log.Infof("new slice size: %d", sliceSize)
		if sliceSize <= 0 {
			return fmt.Errorf("slice size has been set as %v", sliceSize)
		}
		err = cfg.SaveConfig(cfgPath)
		if err != nil {
			return fmt.Errorf("failed to save config file: %v", err)
		}

		var extraFileSliceSize int64
		if len(cfg.ExtraFilePath) != 0 {
			if cfg.ExtraFileSizeInOnePiece == "" {
				return fmt.Errorf("extra file size in one piece is required when extra file path is set")
			}
			extraFileSliceSize, err = units.RAMInBytes(cfg.ExtraFileSizeInOnePiece)
			if err != nil {
				return fmt.Errorf("failed to parse real file size: %v", err)
			}
		}
		if sliceSize+int(extraFileSliceSize) > 32*graphsplit.Gib {
			return fmt.Errorf("slice size %d + extra file slice size %d exceeds 32 GiB", sliceSize, extraFileSliceSize)
		}
		log.Infof("extra file slice size: %d, random rename source file: %v, random select file: %v", extraFileSliceSize, randomRenameSourceFile, randomSelectFile)
		log.Infof("skip filename: %v", skipFilename)
		//ef, err := graphsplit.NewExtraFile(strings.TrimSuffix(cfg.ExtraFilePath, "/"), int64(extraFileSliceSize), int64(sliceSize), randomRenameSourceFile)
		//if err != nil {
		//	return err
		//}
		targetPath := strings.TrimSuffix(c.Args().First(), "/")
		var cb graphsplit.GraphBuildCallback
		if c.Bool("calc-commp") {
			cb = graphsplit.CommPCallback(carDir, c.Bool("rename"), c.Bool("add-padding"))
		} else if c.Bool("save-manifest") {
			cb = graphsplit.CSVCallback(carDir)
		} else {
			cb = graphsplit.ErrCallback()
		}
		videoOutputPath := c.String("video-output-path")
		if videoOutputPath == "" {
			videoOutputPath = targetPath
		}
		vf, err := graphsplit.NewVideoFile(videoPath, videoOutputPath, int64(baseLimit), baseRename)
		if err != nil {
			return err
		}
		//DB
		// 初始化日志（go-log/v2）
		logging.SetLogLevel("graphsplit", "debug") // 或 "debug" 以查看更多日志
		// 创建数据库配置
		config := &graphsplit.DBConfig{
			DSN:             c.String("dsn"),
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxLifetime: time.Hour,
			ConnMaxIdleTime: time.Minute * 30,
			LogLevel:        "warn", // GORM 日志级别
		}

		// 初始化数据库管理器
		mgr, err := graphsplit.NewDBManager(config)
		if err != nil {
			log.Fatalf("Failed to create DBManager: %v", err)
		}
		defer mgr.Close() // 确保关闭连接

		// 执行迁移（创建表和约束）
		if err := mgr.Migrate(); err != nil {
			log.Fatalf("Migration failed: %v", err)
		}
		log.Info("Database migrated successfully.")

		params := graphsplit.ChunkParams{
			ExpectSliceSize: int64(sliceSize),
			ParentPath:      parentPath,
			TargetPath:      targetPath,
			CarDir:          carDir,
			GraphName:       graphName,
			Parallel:        int(parallel),
			Cb:              cb,
			//Ef:                     ef,
			Vf:                     vf,
			RandomRenameSourceFile: randomRenameSourceFile,
			RandomSelectFile:       randomSelectFile,
			SkipFilename:           skipFilename,
			DB:                     mgr,
		}

		loop := c.Bool("loop")
		fmt.Println("loop: ", loop)
		if !loop {
			fmt.Println("chunking once...")
			return graphsplit.Chunk(ctx, &params)
		}
		fmt.Println("loop chunking...")
		for {
			err = graphsplit.Chunk(ctx, &params)
			if err != nil {
				return fmt.Errorf("failed to chunk: %v", err)
			}

			sliceSize++
			cfg.SliceSize = sliceSize
			err = cfg.SaveConfig(cfgPath)
			if err != nil {
				return fmt.Errorf("failed to save config file: %v", err)
			}
			log.Infof("slice size has been set as %d", sliceSize)

			log.Infof("chunking completed! waiting for 60 seconds...")
			<-time.After(60 * time.Second)
		}
	},
}

var restoreCmd = &cli.Command{
	Name:  "restore",
	Usage: "Restore files from CAR files",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "car-path",
			Required: true,
			Usage:    "specify source car path, directory or file",
		},
		&cli.StringFlag{
			Name:     "output-dir",
			Required: true,
			Usage:    "specify output directory",
		},
		&cli.IntFlag{
			Name:  "parallel",
			Value: 4,
			Usage: "specify how many number of goroutines runs when generate file node",
		},
	},
	Action: func(c *cli.Context) error {
		parallel := c.Int("parallel")
		outputDir := c.String("output-dir")
		carPath := c.String("car-path")
		if parallel <= 0 {
			return fmt.Errorf("Unexpected! Parallel has to be greater than 0")
		}

		graphsplit.CarTo(carPath, outputDir, parallel)
		graphsplit.Merge(outputDir, parallel)

		fmt.Println("completed!")
		return nil
	},
}

var commpCmd = &cli.Command{
	Name:  "commP",
	Usage: "PieceCID and PieceSize calculation",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "rename",
			Value: false,
			Usage: "rename carfile to piece",
		},
		&cli.BoolFlag{
			Name:  "add-padding",
			Value: false,
			Usage: "add padding to carfile in order to convert it to piece file",
		},
	},
	Action: func(c *cli.Context) error {
		ctx := context.Background()
		targetPath := c.Args().First()

		res, err := graphsplit.CalcCommP(ctx, targetPath, c.Bool("rename"), c.Bool("add-padding"))
		if err != nil {
			return err
		}

		fmt.Printf("PieceCID: %s, PieceSize: %d\n", res.Root, res.Size)
		return nil
	},
}

var importDatasetCmd = &cli.Command{
	Name:  "import-dataset",
	Usage: "import files from the specified dataset",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "dsmongo",
			Required: true,
			Usage:    "specify the mongodb connection",
		},
	},
	Action: func(c *cli.Context) error {
		ctx := context.Background()

		targetPath := c.Args().First()
		if !graphsplit.ExistDir(targetPath) {
			return fmt.Errorf("Unexpected! The path to dataset does not exist")
		}

		return dataset.Import(ctx, targetPath, c.String("dsmongo"))
	},
}
