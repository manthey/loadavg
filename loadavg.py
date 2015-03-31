#!/usr/bin/python

import copy
import json
import multiprocessing
import os
import pprint
import select
import servicemanager
import socket
import sys
import threading
import time
import weakref
import win32pdh
import win32event
import win32service
import win32serviceutil


DefaultPort = 24047

        
avgWeights = {
    1:  {"bits": 11, "w": 1884},  # 1884 = (1<<11)/exp(5sec/60sec)
    5:  {"bits": 11, "w": 2014},  # 2014 = (1<<11)/exp(5sec/300sec)
    15: {"bits": 11, "w": 2037},  # 2037 = (1<<11)/exp(5sec/900sec)
}


class LoadAvgCollect(threading.Thread):
    """Start polling different performance counters to keep track of process
     and disk load."""
    def __init__(self, verbose=0):
        """Initialize the thread.
        Enter: verbose: if non-zero, print to stdout."""
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.halt = False
        self.state = {}
        self.history = {}
        self.maxhistory = 86400 * 7 / 5  # 1 week
        self.verbose = verbose
        self.lock = threading.Lock()
        self.fields = {
            "load": r"\System\Processor Queue Length",
            "diskload": r"\LogicalDisk(_Total)\Current Disk Queue Length",
            "numproc": r"\System\Processes",
            "percent": r"\Processor(_Total)\% Processor Time",
            }

    def format(self, state=None, mode="load"):
        """Generate a loadavg string for the process load or the disk load.
        Enter: state: if provided, a dictionary of the state to format.  If
                      None, use the current state.
               mode: "disk" or "diskload" formats the disk load.  "proc" or
                     "procload" formats the process load.  Anything else
                     formats the combined load."""
        if not state:
            state = self.get()
        return formatState(state, mode)

    def get(self):
        """Get the current state of the data collection.
        Exit:  state: state dictionary."""
        self.lock.acquire()
        state = copy.copy(self.state)
        self.lock.release()
        return state

    def getHistory(self):
        """Get the current history of the data collection.
        Exit:  history: history dictionary."""
        self.lock.acquire()
        history = copy.copy(self.history)
        self.lock.release()
        return history

    def run(self):
        """Collect the data."""
        nexttime = time.time()
        interval = 5
        qhandles = {}
        query = win32pdh.OpenQuery()
        for key in self.fields:
            qhandles[key] = win32pdh.AddCounter(query, self.fields[key])
        win32pdh.CollectQueryData(query)
        cores = multiprocessing.cpu_count()
        time.sleep(1)
        loadFromPercent = 0
        while not self.halt:
            curtime = time.time()
            self.lock.acquire()
            state = copy.copy(self.state)
            self.lock.release()
            win32pdh.CollectQueryData(query)
            for key in self.fields:
                chandle = qhandles[key]
                if key == "percent":
                    try:
                        (ctype, value) = win32pdh.GetFormattedCounterValue(
                            chandle, win32pdh.PDH_FMT_DOUBLE)
                    except:
                        value = 0
                else:
                    (ctype, value) = win32pdh.GetFormattedCounterValue(
                        chandle, win32pdh.PDH_FMT_LONG)
                state[key] = value
            loadFromPercent += state["percent"] * 0.01 * cores
            if loadFromPercent >= 1:
                state["load"] += int(loadFromPercent)
                loadFromPercent -= int(loadFromPercent)
            state["residual"] = loadFromPercent
            for key in ("load", "diskload"):
                value = state[key]
                for avg in avgWeights:
                    akey = key+"_"+str(avg)
                    cur = state.get(akey, 0)
                    bits = avgWeights[avg]["bits"]
                    w = avgWeights[avg]["w"]
                    new = (cur*w+((1 << bits) - w) * (value << bits)) >> bits
                    state[akey] = new
            state["time"] = curtime
            self.lock.acquire()
            for key in state:
                self.state[key] = state[key]
                if not key in self.history:
                    self.history[key] = []
                self.history[key] = self.history[key][-self.maxhistory:]
                self.history[key].append(state[key])
            self.lock.release()
            if self.verbose >= 2:
                print "%5.3f %s" % (curtime, self.format(state, "load"))
            if self.verbose >= 3:
                print "               %s" % self.format(state, "proc")
                print "               %s" % self.format(state, "disk")
            if self.verbose >= 4:
                pprint.pprint(state)
            nexttime += interval
            delay = nexttime-time.time()
            if delay > 0:
                time.sleep(delay)
            else:
                state["missed"] = state.get("missed", 0)+1
                state["last_miss"] = -delay
                state["total_miss"] = state.get("total_miss", 0)-delay
        win32pdh.CloseQuery(query)


class LoadAvgService(threading.Thread):
    """Create a socket service.  When a caller connects to the socket,
     receive a command word, send a response, and disconnect that caller."""
    def __init__(self, verbose=0, port=DefaultPort, collector=None):
        """Initialize the thread.
        Enter: verbose: if non-zero, print to stdout.
               port: the port to listen on.
               collector: the collector object to query or command."""
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.halt = False
        self.verbose = verbose
        self.port = port
        self.collector = weakref.proxy(collector)

    def handle_client(self, sock, addr):
        """Given a client socket, read a command word and send a response.
        Enter: sock: client socket to read and write.
               addr: client address."""
        timeout = 15
        rs, ws, es = select.select([sock], [], [], timeout)
        if not len(rs):
            return
        data = sock.recv(4096)
        cmd = data.split(None, 1)[0]
        if not len(cmd):
            return
        if cmd == "end":
            self.collector.halt = True
            result = "okay"
        elif cmd in ("load", "disk", "proc"):
            result = self.collector.format(mode=cmd)
        elif cmd == "status":
            state = self.collector.get()
            result = pprint.pformat(state).strip()
        elif cmd == "history":
            history = self.collector.getHistory()
            result = json.dumps(history)
        else:
            result = "failed - unknown command"
        rs, ws, es = select.select([], [sock], [], timeout)
        if not len(ws):
            return
        sock.send(result)
        if cmd == "end":
            self.halt = True

    def run(self):
        """Listen on a socket until asked to stop."""
        sock = None
        if self.verbose >= 2:
            print "Accepting tcp connections on port", self.port
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(0)
            sock.bind(("127.0.0.1", self.port))
            sock.listen(10)
        except:
            self.halt = True
            return
        while not self.halt:
            timeout = 5  # Amount of time for us to notice a halt
            rs, ws, es = select.select([sock], [], [], timeout)
            if not len(rs):
                continue
            try:
                (clientsock, addr) = sock.accept()
            except:
                continue
            clientsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            clientsock.setblocking(0)
            self.handle_client(clientsock, addr)
            clientsock = None  # let it close on its own


class LoadAvgSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "LoadAvgSvc"
    _svc_display_name_ = "Loadavg for Windows Service"

    import loadavg
    svcPath = (os.path.splitext(os.path.abspath(loadavg.__file__))[0] + '.' +
               _svc_name_)

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.waitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.service = None

    def SvcStop(self):
        if self.service:
            self.service.halt = True
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.waitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.collector = LoadAvgCollect()
        self.collector.start()
        self.service = LoadAvgService(0, DefaultPort, self.collector)
        self.service.start()
        while not self.service.halt:
            time.sleep(5)


def formatState(state, mode="load"):
    """Generate a loadavg string for the process load or the disk load.
    Enter: state: a dictionary of the state to format.
           mode: "disk" or "diskload" formats the disk load.  "proc" or
                 "procload" formats the process load.  Anything else formats
                 the combined load."""
    if mode in ("disk", "diskload"):
        return "%4.2f %4.2f %4.2f %d" % (
            float(state["diskload_1"]) / (1 << avgWeights[1]["bits"]),
            float(state["diskload_5"]) / (1 << avgWeights[5]["bits"]),
            float(state["diskload_15"]) / (1 << avgWeights[15]["bits"]),
            state["diskload"])
    if mode in ("proc", "procload"):
        return "%4.2f %4.2f %4.2f %d/%d" % (
            float(state["load_1"]) / (1 << avgWeights[1]["bits"]),
            float(state["load_5"]) / (1 << avgWeights[5]["bits"]),
            float(state["load_15"]) / (1 << avgWeights[15]["bits"]),
            state["load"],
            state["numproc"])
    return "%4.2f %4.2f %4.2f %d/%d" % (
        (float(state["load_1"] + state["diskload_1"]) /
         (1 << avgWeights[1]["bits"])),
        (float(state["load_5"] + state["diskload_5"]) /
         (1 << avgWeights[5]["bits"])),
        (float(state["load_15"] + state["diskload_15"]) /
         (1 << avgWeights[15]["bits"])),
        state["load"]+state["diskload"],
        state["numproc"])


def query_service(cmd="load", port=DefaultPort):
    """Query the running service.
    Enter: cmd: the command to send to the service.
           port: the port of the service to query.
    Exit:  result: the reply from the service."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error:
        return "Failed - can't create socket"
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.connect(("127.0.0.1", port))
    except socket.error:
        return "Failed - can't connect socket"
    timeout = 5
    rs, ws, es = select.select([], [sock], [], timeout)
    if not len(ws):
        return "Failed - socket not receiving"
    sock.send(cmd)
    data = []
    while True:
        rs, ws, es = select.select([sock], [], [], timeout)
        if not len(rs):
            break
        data.append(sock.recv(4096))
        if not len(data[-1]):
            break
        timeout = 0
    return ("".join(data)).strip()


def query_service_loop(cmd="load", opts={}, interval=5, collector=None,
                       verbose=0):
    """Query the running or internal service repeatedly.
    Enter: cmd: the command to send to the service.
           opts: a dictionary of additional options, possible including port,
                 count, and old.
           interval: frequency to query the service.
           collector: if not None, get the status from this collector rather
                      than from a service port.
           verbose: verbosity level."""
    nexttime = time.time()
    if opts.get("old", 0) > 0 and not collector:
        try:
            history = json.loads(query_service("history", opts.get(
                "port", DefaultPort)))
        except Exception:
            history = {}
        if "time" in history and len(history["time"]):
            oldtime = nexttime - interval * opts["old"]
            pos = -len(history["time"])
            while oldtime < nexttime - interval * 0.5:
                if oldtime >= history["time"][pos]:
                    while (pos + 1 < 0 and
                            oldtime >= history["time"][pos + 1]):
                        pos += 1
                    state = {}
                    for key in history:
                        if len(history[key]) >= -pos:
                            state[key] = history[key][pos]
                    print "%s %s" % (time.strftime(
                        "%H:%M:%S", time.localtime(oldtime)),
                        formatState(state))
                oldtime += interval
    first = True
    count = opts.get("count", 0)
    while True:
        try:
            if not first or cmd != "run":
                curtime = time.time()
                if not collector:
                    val = query_service(cmd, opts.get("port", DefaultPort))
                else:
                    val = collector.format(mode=cmd)
                if verbose:
                    print "%s %s" % (time.strftime(
                        "%H:%M:%S", time.localtime(curtime)), val)
                else:
                    sys.stdout.write('\r' + val)
                    sys.stdout.flush()
            first = False
            nexttime += interval
            if count:
                count -= 1
                if count <= 0:
                    break
            delay = nexttime-time.time()
            if delay > 0:
                time.sleep(delay)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    help = False
    mode = "load"
    frequency = None
    collector = None
    windowsService = False
    opts = {"port": DefaultPort}
    verbose = 1
    for arg in sys.argv[1:]:
        if ((arg[:2] == "--" or arg[:1] == "/") and arg.lstrip("-/") in
                ("disk", "load", "proc", "service", "end", "run", "status",
                 "history")):
            mode = arg.lstrip("-/")
        elif ((arg[:2] == "--" or arg[:1] == "/") and "=" in arg and
                arg.lstrip("-/").split("=", 1)[0] in ("count", "old", "port")):
            parts = arg.lstrip("-/").split("=", 1)
            opts[parts[0]] = int(parts[1])
        elif arg in ("--browse", "/browse"):
            def print_counter(counter):
                print counter
            win32pdh.BrowseCounters(
                None, 0, print_counter, win32pdh.PERF_DETAIL_WIZARD,
                "Counter List")
        elif arg in ("-q", "/q", "--quiet", "/quiet"):
            verbose -= 1
        elif arg in ("-v", "/v", "--verbose", "/verbose"):
            verbose += 1
        elif arg in ['install', 'remove', 'start', 'stop']:
            windowsService = True
        elif not frequency and arg.isdigit():
            frequency = int(arg)
        else:
            help = True
    if help:
        print """Run or query a loadavg service on windows.

Syntax: loadavg.py install|remove|start|stop
  --load|--disk|--proc|--service|--end|--run|--status|--history
  (frequency) --count=(count) --old=(count)
  --port=(port) --browse -q -v

If one of install, remove, start, or stop is used, this program will be treated
  as a Windows service.  The other command line parameters are ignored.
--browse shows the performance counters browse dialog.
--count specifies how many records to print when a frequency is specified.
--disk queries the disk load average.
--end asks a running service to stop.
--history prints the internal history state.
--load queries the combined process and disk load average (this is the default
  action).
--old specifies the number of older values to print.  Older values require that
  enough history has been collected, and will be spaced at the requested
  frequency.  This option is only used for --load, --proc, or --disk when a
  frequency or --count is specified.
--port specifies the port the service and queries should run on.  Default is
 24047.
--proc queries the process load average.
-q or --quiet decreases the verbosity.
--run monitors the load and prints it to stdout without providing a service.
--service starts the service running.  This will continue to run until asked to
  end.
--status prints the internal state dictionary.
-v or --verbose increases the verbosity.

If a frequency is specified and one of --load, --proc, --disk, or --run is
specified, the appropriate load average is printed at that frequency.

On linux, loadavg samples the system every 5 seconds.  The values shown in proc
are exponential decay values of the number of processes that were waiting,
weighted over 1, 5, and 15 minutes.  This mimics that behavior exactly, using
the Windows performance counter of processor queue length.  It also has a
similar set of values for disk queue.  The value shown for load is the same as
that shown in linux, except the most recent pid is not shown.

This needs to run in the background (as a service, for instance) with the
--service flag.  It can then be run as desired to get the current loadavg
values.

If this fails because the expected performance counters are unavailable, try
running the following:
  cd C:\\Windows\\System32
  lodctr /R"""
        sys.exit(0)
    if windowsService:
        win32serviceutil.HandleCommandLine(
            LoadAvgSvc, argv=sys.argv, serviceClassString=LoadAvgSvc.svcPath)
        sys.exit(0)
    if ("count" in opts or "old" in opts) and not frequency:
        frequency = 5
    if mode == "run" and verbose < 2 and not frequency:
        verbose = 2
    if mode == "service" or mode == "run":
        collector = LoadAvgCollect(verbose=verbose)
        collector.start()
    if mode == "service":
        service = LoadAvgService(verbose, opts["port"], collector)
        service.start()
    if mode == "service" or (mode == "run" and not frequency):
        while not collector.halt:
            time.sleep(5)
    elif frequency:
        query_service_loop(mode, opts, frequency, collector, verbose)
    else:
        print query_service(mode, opts["port"])
