package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	// TCP address to listen on
	defaultAddr = ":8080"

	// service endpoint
	endpoint  = "/numbers"
	urlGetKey = "u"

	// request method for outgoing http requests
	requestMethod = http.MethodGet

	// number of workers(goroutines) which will process outgoing http requests
	defaultWorkersCnt = 100

	// we need to have a small time gap (10ms-50ms) to be able to respond in case we're timeouted
	defaultTimeout = time.Millisecond * 490

	// maximum duration for reading the entire request, including the body. protection from slow clients
	readTimeout = time.Millisecond * 60
)

var (
	//channel with the tasks
	tasks chan *task

	//timeout describes maximum duration of processing number providers
	timeout *time.Duration

	//workerCnt is a number of goroutines which will process http requests
	workerCnt *int

	//service http listen address
	listenAddr *string

	//supported protocols of the number providers. protocolName: enabled?
	supportedProtocols = map[string]bool{
		"http":  true,
		"https": true,
		"ftp":   false,
	}
)

// structure of the response from the number provider
type sourceResponse struct {
	Numbers []int
}

// task is a struct containing a single url to fetch data from
type task struct {
	context      context.Context
	url          string
	waitingGroup *sync.WaitGroup
	resCh        chan []int
}

// TreeNode is a node of a binary tree
type TreeNode struct {
	Left  *TreeNode
	Value int
	Right *TreeNode
}

func init() {
	listenAddr = flag.String("listen", defaultAddr, "http listen address")
	workerCnt = flag.Int("workers", defaultWorkersCnt, "number of http client workers")

	timeout = flag.Duration("timeout", defaultTimeout, "maximum duration of processing number providers")

	flag.Parse()
}

func main() {
	bgProcWg := &sync.WaitGroup{}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	tasks = make(chan *task, *workerCnt)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	bgProcWg.Add(1)
	go runWorkers(ctx, bgProcWg, *workerCnt)
	log.Printf("using %d http workers", *workerCnt)
	log.Printf("timeout set to %v", *timeout)

	mux := http.NewServeMux()
	mux.HandleFunc(endpoint, serveNumbers)
	server := http.Server{
		Addr:        *listenAddr,
		Handler:     mux,
		ReadTimeout: readTimeout,
	}
	ln, err := net.Listen("tcp", server.Addr) // will be closed by server.Shutdown()
	if err != nil {
		log.Fatalf("could not listen to %s: %v", server.Addr, err)
		os.Exit(1)
	}

	bgProcWg.Add(1)
	go func() {
		defer bgProcWg.Done()
		if err := server.Serve(ln); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("could not start http server: %v", err)
				os.Exit(1)
			}
		}
	}()
	log.Printf("listening on %s", *listenAddr)

	sig := <-sigs
	log.Printf("got %v signal. Gracefully shutting down", sig)
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("could not shutdown http server: %v", err)
	}
	cancel()
	bgProcWg.Wait()
	close(tasks)
}

// worker processes tasks from the tasks channel
func worker(ctx context.Context, wg *sync.WaitGroup, tasks <-chan *task, client *http.Client) {
	defer wg.Done()

	for {
		select {
		case t, ok := <-tasks:
			if !ok {
				return
			}
			fetchURL(client, t)
		case <-ctx.Done():
			return
		}
	}
}

// run workers as goroutines
func runWorkers(ctx context.Context, wg *sync.WaitGroup, workers int) {
	defer wg.Done()
	client := &http.Client{
	// no need to set timeout here. context will deal with timeouts
	}

	workersWg := &sync.WaitGroup{} // workers' waiting group
	workersWg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker(ctx, workersWg, tasks, client)
	}
	workersWg.Wait()
}

// insert inserts value into the tree
func insert(ctx context.Context, n *TreeNode, v int) *TreeNode {
	if n == nil {
		return &TreeNode{
			Left:  nil,
			Value: v,
			Right: nil,
		}
	}
	select {
	case <-ctx.Done():
		return n
	default:
	}

	if v < n.Value {
		n.Left = insert(ctx, n.Left, v)
	} else if v > n.Value {
		n.Right = insert(ctx, n.Right, v)
	}

	return n
}

// walk walks throw the tree in the ascending order and returns slice of the node values
func (n *TreeNode) walk(ctx context.Context) []int {
	s := make([]int, 0)
	select {
	case <-ctx.Done():
		return s
	default:
	}

	if n == nil {
		return s
	}
	if n.Left != nil {
		s = append(s, n.Left.walk(ctx)...)
	}
	s = append(s, n.Value)
	if n.Right != nil {
		s = append(s, n.Right.walk(ctx)...)
	}

	return s
}

// extractURLs extracts urls from the GET parameters
func extractURLs(sURLs *url.URL) []string {
	result := make([]string, 0)

	urls, ok := sURLs.Query()[urlGetKey]
	if len(urls) == 0 || !ok {
		return result
	}

	for _, u := range urls {
		pu, err := url.Parse(u)
		if err != nil {
			log.Printf("could not parse url %q: %v", u, err)
			continue
		}

		if protocolEnabled, ok := supportedProtocols[pu.Scheme]; !ok || !protocolEnabled {
			log.Printf("protocol %q is not supported", pu.Scheme)
			continue
		}

		result = append(result, u)
	}

	return result
}

// serveNumbers serves the incoming http requests
func serveNumbers(w http.ResponseWriter, r *http.Request) {
	var result *TreeNode

	requestWg := &sync.WaitGroup{}
	requestWg.Add(1)

	tasksWg := &sync.WaitGroup{}
	done := make(chan struct{})
	resCh := make(chan []int, *workerCnt)

	ctx, cancel := context.WithTimeout(context.TODO(), *timeout)
	defer cancel()

	func() {
		urls := extractURLs(r.URL)
		for _, u := range urls {
			tasksWg.Add(1)
			select {
			case <-ctx.Done():
				tasksWg.Done()
				return
			case tasks <- &task{
				context:      ctx,
				url:          u,
				waitingGroup: tasksWg,
				resCh:        resCh,
			}:
			}
		}
	}()

	go func() {
		tasksWg.Wait()
		close(done)
		close(resCh) // we need to wait for all the worker goroutines finish before closing the channel
		requestWg.Done()
	}()

	//synchronously waiting for the results from the go routines or timeout-ing
	func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case res, ok := <-resCh:
				if !ok {
					return
				}
				for _, v := range res {
					select {
					case <-ctx.Done():
						return
					default:
						result = insert(ctx, result, v)
					}
				}
			}
		}
	}()

	w.Header().Set("Content-Type", "application/json")

	// http.StatusOK will be written implicitly on response write
	err := json.NewEncoder(w).Encode(map[string]interface{}{"number": result.walk(ctx)})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("could not encode response: %v", err)
	}

	requestWg.Wait()
}

//fetchURL fetches numbers from the sURL and puts them into resCh
func fetchURL(client *http.Client, t *task) {
	defer t.waitingGroup.Done()
	select {
	case <-t.context.Done():
		return
	default:
	}

	request, err := http.NewRequest(requestMethod, t.url, nil)
	if err != nil {
		log.Printf("%q: could not create request for: %v", t.url, err)
		return
	}
	request = request.WithContext(t.context)
	response, err := client.Do(request)
	if err != nil {
		uErr, ok := err.(*url.Error)
		if ok && uErr.Err == context.DeadlineExceeded {
			// just skip timeout-ed requests
			return
		}
		log.Printf("%q: request error: %v", t.url, err)

		return
	}
	defer func() {
		bErr := response.Body.Close()
		if bErr != nil {
			log.Printf("%q: could not close response body: %v", t.url, bErr)
		}
	}()

	var res sourceResponse
	err = json.NewDecoder(response.Body).Decode(&res)
	if err != nil {
		log.Printf("%q: response parse error: %v", t.url, err)
		return
	}

	select {
	case <-t.context.Done():
		return
	case t.resCh <- res.Numbers:
	}
}
