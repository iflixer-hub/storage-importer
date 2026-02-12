package main

type Job struct {
	FileID        uint64
	SourceMaster  string
	R2Prefix      string
	R2MasterKey   string
	AttemptNumber int
}
