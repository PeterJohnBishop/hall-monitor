package main

import (
	"github.com/peterjohnbishop/hall-monitor/monitor"
)

func main() {
	db := monitor.InitializeDB()
	monitor.ChromiumMonitor(db)
}
