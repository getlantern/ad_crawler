build:
	go build -ldflags="-s -w -extldflags=-static"  -tags osusergo,netgo,sqlite_omit_load_extension -o ../partner_crawler

