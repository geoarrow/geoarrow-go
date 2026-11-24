// Command fetchtestdata downloads GeoArrow reference files from
// https://github.com/geoarrow/geoarrow-data into a local directory so tests
// can check this implementation against the files other GeoArrow
// implementations use.
//
// The downloaded files are committed under testdata/geoarrow-data, so this is
// a maintenance tool for refreshing or extending them, not something tests or
// CI depend on. It is wired up through go generate (see generate.go) and only
// downloads files that are missing:
//
//	go generate ./...
//	go run ./internal/fetchtestdata -force   # re-download everything
//
// The release tag is pinned so test inputs only change when this file does.
// Update testdata/geoarrow-data/README.md alongside any change here.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// geoarrowDataTag is the geoarrow-data release the example files come from.
	geoarrowDataTag = "v0.2.0"
	baseURL         = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/" + geoarrowDataTag + "/"
	exampleDir      = "example/files/"
)

// extraFiles are always fetched, as paths relative to the repository root.
// This is real-world geoarrow.box data: Natural Earth country bounds, which
// include boxes crossing the antimeridian.
var extraFiles = []string{
	"natural-earth/files/natural-earth_countries-bounds_box.arrows",
}

// dimSuffixes are the file name suffixes for the XY, XYZ, XYM and XYZM
// variants of each example.
var dimSuffixes = []string{"", "-z", "-m", "-zm"}

// variantSuffixes are the encodings published for each example: separated
// struct coords (""), interleaved coords, and the WKT and WKB encodings.
var variantSuffixes = []string{"", "_interleaved", "_wkt", "_wkb"}

// defaultTypes are the geometry types fetched when -types is not given.
const defaultTypes = "point,linestring,polygon,multipoint,multilinestring,multipolygon"

func main() {
	out := flag.String("out", "testdata/geoarrow-data", "directory to write files into")
	types := flag.String("types", defaultTypes, "comma-separated geometry types to fetch (e.g. point,polygon,linestring)")
	force := flag.Bool("force", false, "re-download files that already exist")
	flag.Parse()

	if err := run(*out, strings.Split(*types, ","), *force); err != nil {
		fmt.Fprintln(os.Stderr, "fetchtestdata:", err)
		os.Exit(1)
	}
}

// fileNames lists the repository-relative path of every file to fetch: for
// each geometry type and dimension, one .tsv of WKT source rows and one
// .arrows IPC stream per encoding, plus extraFiles.
func fileNames(types []string) []string {
	var names []string
	for _, typ := range types {
		typ = strings.TrimSpace(typ)
		if typ == "" {
			continue
		}
		for _, dim := range dimSuffixes {
			base := exampleDir + "example_" + typ + dim
			names = append(names, base+".tsv")
			for _, variant := range variantSuffixes {
				names = append(names, base+variant+".arrows")
			}
		}
	}
	return append(names, extraFiles...)
}

func run(outDir string, types []string, force bool) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	client := &http.Client{Timeout: 60 * time.Second}
	names := fileNames(types)

	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		errs        []error
		fetched     int
		sem         = make(chan struct{}, 8)
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()

	for _, name := range names {
		dest := filepath.Join(outDir, path.Base(name))
		if _, err := os.Stat(dest); err == nil && !force {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := download(ctx, client, baseURL+name, dest)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				return
			}
			fetched++
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Fprintln(os.Stderr, err)
		}
		return fmt.Errorf("%d of %d downloads failed", len(errs), len(names))
	}
	fmt.Printf("geoarrow-data %s: %d downloaded, %d already present in %s\n",
		geoarrowDataTag, fetched, len(names)-fetched, outDir)
	return nil
}

// download fetches url into dest, writing through a temp file so an
// interrupted download never leaves a truncated file behind.
func download(ctx context.Context, client *http.Client, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }() // body is fully read or abandoned
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dest), ".download-*")
	if err != nil {
		return err
	}
	// After a successful rename the temp file no longer exists, so this only
	// matters on failure paths; a remove error there is not actionable.
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		return errors.Join(err, tmp.Close())
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dest)
}
