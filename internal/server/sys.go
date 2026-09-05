package server

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

// sysSample is one 1 Hz reading of process + host resource usage. The
// engine and the services are one process (chronod), so RSS *is* the
// "engine + services" RAM figure. All floats are finite (sanitized
// below); byte counts are >= 0 except DBBytes (-1 = file unknown).
type sysSample struct {
	RSSBytes       uint64  // process resident set
	HeapBytes      uint64  // runtime.MemStats.HeapAlloc (in-use Go heap)
	HostMemUsed    uint64  // host RAM in use
	HostMemTotal   uint64  // host RAM total
	HostCPUPercent float64 // host busy across all cores, 0..100
	ProcCPUPercent float64 // this process, % of one core (may exceed 100)
	Cores          int     // logical cores (runtime.NumCPU)
	DBBytes        int64   // alert-store file size; -1 unknown, 0 no path
	DiskFreeBytes  uint64  // free on the db file's filesystem
	DiskTotalBytes uint64  // size of that filesystem
}

// sysSampler produces one sysSample per call. gopsutil's interval=0 CPU
// calls (cpu.Percent, Process.CPUPercent) keep package/instance-level
// last-sample windows that are not safe for concurrent callers, and
// /stats can sample off-tick — hence the mutex serializing every call.
type sysSampler struct {
	mu     sync.Mutex
	dbPath string // "" disables DB/disk fields
	proc   *process.Process
}

func newSysSampler(dbPath string) *sysSampler {
	sp := &sysSampler{dbPath: dbPath}
	// Self-lookup cannot realistically fail; a nil proc just zeroes the
	// process fields rather than taking the dashboard down.
	sp.proc, _ = process.NewProcess(int32(os.Getpid()))
	return sp
}

// sample takes one reading. Failures degrade to zero values: the
// snapshot must always exist and always marshal (runTick's broadcast
// depends on it), never error.
func (sp *sysSampler) sample() sysSample {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	s := sysSample{Cores: runtime.NumCPU()}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms) // µs-scale STW, fine at 1 Hz
	s.HeapBytes = ms.HeapAlloc

	if vm, err := mem.VirtualMemory(); err == nil {
		s.HostMemUsed, s.HostMemTotal = vm.Used, vm.Total
	}
	if pct, err := cpu.Percent(0, false); err == nil && len(pct) > 0 {
		s.HostCPUPercent = finite(pct[0])
	}
	if sp.proc != nil {
		if mi, err := sp.proc.MemoryInfo(); err == nil && mi != nil {
			s.RSSBytes = mi.RSS
		}
		if pct, err := sp.proc.CPUPercent(); err == nil {
			s.ProcCPUPercent = finite(pct)
		}
	}
	if sp.dbPath != "" {
		// The bbolt file size IS the disk cost: it pre-allocates in
		// pages and the freelist reuses them, so no store internals
		// are needed — stat the file, statfs its directory.
		if st, err := os.Stat(sp.dbPath); err == nil {
			s.DBBytes = st.Size()
		} else {
			s.DBBytes = -1
		}
		if du, err := disk.Usage(filepath.Dir(sp.dbPath)); err == nil {
			s.DiskFreeBytes, s.DiskTotalBytes = du.Free, du.Total
		}
	}
	return s
}

// finite clamps non-finite floats to 0: encoding/json refuses NaN/Inf,
// and a bad reading would make runTick skip the whole SSE broadcast.
func finite(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}
