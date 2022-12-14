# Learned index benchmarking in Go

This repository is a reimplementation of several indexes in the SOSD benchmark in Go.

To run this, Go 1.19 needs to be installed.

First ./download.sh needs to be run to download the SOSD datasets. These
datasets will be downloaded into the `data/` directory.

Then simply running `go run main.go` will run the benchmarks and
output the results to a set of CSV files in the base directory. Results will contain 4 columns:
index name, index build time, index size, and average lookup latency.