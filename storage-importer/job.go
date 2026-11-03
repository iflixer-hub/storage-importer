package main

type Job struct {
	FileID        uint64
	SourceMaster  string
	R2Prefix      string
	R2MasterKey   string
	AttemptNumber int
	// qualities already imported by earlier attempts ("480/index.m3u8"), they are not imported again
	Qualities []string
}
