

Generating protobufs:
```sh
cd ingestion/
wget https://github.com/protocolbuffers/protobuf/releases/download/v26.1/protoc-26.1-linux-x86_64.zip
unzip protoc-26.1-linux-x86_64.zip -d protoc
rm protoc-26.1-linux-x86_64.zip
./protoc/bin/protoc -I=$PWD --python_out=. --pyi_out=. $PWD/BusPosition.proto
```

Code inspired by articles:
https://blog.devgenius.io/bigquery-streaming-in-python-db674a6e4a6a
https://medium.com/@thakur.ritesh19/streaming-data-to-google-bigquery-using-storage-write-api-c36fb3af8600
