SET CGO_ENABLED=0
SET GOOS=linux
SET GOARCH=amd64
cd .\app\datalink
go build main.go -o data-link