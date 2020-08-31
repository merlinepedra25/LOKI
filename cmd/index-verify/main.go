package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	cortexstorage "github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/grafana/loki/pkg/cfg"
	"github.com/grafana/loki/pkg/chunkenc"
	"github.com/grafana/loki/pkg/loki"
	"github.com/grafana/loki/pkg/storage"
	"github.com/grafana/loki/pkg/util"
)

const (
	chunkTimeRangeKeyV1a = 1
	chunkTimeRangeKeyV1  = '1'
	chunkTimeRangeKeyV2  = '2'
	chunkTimeRangeKeyV3  = '3'
	chunkTimeRangeKeyV4  = '4'
	chunkTimeRangeKeyV5  = '5'
	metricNameRangeKeyV1 = '6'

	// For v9 schema
	seriesRangeKeyV1      = '7'
	labelSeriesRangeKeyV1 = '8'
	// For v11 schema
	labelNamesRangeKeyV1 = '9'
)

type stream struct {
	seriesId string
	labels   model.LabelSet
	chunks   []string
}

type Config struct {
	loki.Config  `yaml:",inline"`
	printVersion bool
	printConfig  bool
	logConfig    bool
	configFile   string
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.printVersion, "version", false, "Print this builds version information")
	f.BoolVar(&c.printConfig, "print-config-stderr", false, "Dump the entire Loki config object to stderr")
	f.BoolVar(&c.logConfig, "log-config-reverse-order", false, "Dump the entire Loki config object at Info log "+
		"level with the order reversed, reversing the order makes viewing the entries easier in Grafana.")
	f.StringVar(&c.configFile, "config.file", "", "yaml file to load")
	c.Config.RegisterFlags(f)
}

// Clone takes advantage of pass-by-value semantics to return a distinct *Config.
// This is primarily used to parse a different flag set without mutating the original *Config.
func (c *Config) Clone() flagext.Registerer {
	return func(c Config) *Config {
		return &c
	}(*c)
}

// Assumptions
// Only takes most recent schema
// Doesn't check to make sure chunks are in the time range, index will return chunks past `through`

func main() {

	var config Config
	if err := cfg.Parse(&config); err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing config: %v\n", err)
		os.Exit(1)
	}
	//limits, err := validation.NewOverrides(config.LimitsConfig, nil)
	//s, err := storage.NewStore(config.StorageConfig, config.ChunkStoreConfig, config.SchemaConfig, limits)
	//if err != nil {
	//	panic(err)
	//}

	//fr := time.Date(2020, 4, 17, 00, 00, 00, 00, time.UTC)
	fr := time.Now().Add(-168 * time.Hour)
	//to := time.Date(2020, 4, 17, 00, 01, 00, 00, time.UTC)
	to := time.Now()

	err := config.SchemaConfig.Validate()
	if err != nil {
		log.Fatal("Failed to validate schema:", err)
	}
	cfgs := config.SchemaConfig.Configs
	sc, err := config.SchemaConfig.Configs[len(cfgs)-1].CreateSchema()

	from, through := util.RoundToMilliseconds(fr, to)
	nameLabelMatcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, "logs")
	if err != nil {
		log.Fatal("Failed to create metric name matcher:", err)
	}

	storage.RegisterCustomIndexClients(&config.StorageConfig, prometheus.DefaultRegisterer)

	ic, err := cortexstorage.NewIndexClient(config.SchemaConfig.Configs[len(cfgs)-1].IndexType, config.StorageConfig.Config, config.SchemaConfig.SchemaConfig, prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatal("Failed to create index client:", err)
	}

	cc, err := cortexstorage.NewChunkClient(config.SchemaConfig.Configs[len(cfgs)-1].ObjectType, config.StorageConfig.Config, config.SchemaConfig.SchemaConfig, prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatal("Failed to create index client:", err)
	}

	ctx := context.Background()

	//////////////////
	// Get Streams
	/////////////////
	start := time.Now()
	iqs, err := sc.GetReadQueriesForMetric(from, through, "fake", nameLabelMatcher.Value)
	if err != nil {
		log.Fatal("failed to get index queries for metric:", err)
	}
	entries, err := queryIndex(ctx, iqs, ic)
	if err != nil {
		log.Fatal("Failed to query index: ", err)
	}

	//matcher, err := labels.NewMatcher(labels.MatchEqual, "job", "shell")
	//if err != nil {
	//	log.Fatal("Failed to create matcher", err)
	//}

	series, err := parseIndexEntries(ctx, entries, nil)
	if err != nil {
		log.Fatal("Failed to parse index entry:", err)
	}

	log.Println("Found series:", len(series))

	// Maps a label name to a series and value for that series
	//labelMap := map[model.LabelName]map[string]model.LabelValue{}

	// Output List
	chunks := []string{}

	for _, s := range series {

		//////////////////
		// Get Chunks
		/////////////////

		iqs, err = sc.(chunk.SeriesStoreSchema).GetChunksForSeries(from, through, "fake", []byte(s))
		chunkEntries, err := queryIndex(ctx, iqs, ic)
		if err != nil {
			log.Fatal("Failed to query index: ", err)
		}
		chks, err := parseIndexEntries(ctx, chunkEntries, nil)
		if err != nil {
			log.Fatal("Failed to parse index entry:", err)
		}
		if len(chks) == 0 {
			continue
		}
		chunks = append(chunks, chks...)

		//////////////////////
		//// Get Label Names
		/////////////////////
		//start = time.Now()
		//iqs, err = sc.(chunk.SeriesStoreSchema).GetLabelNamesForSeries(from, through, "29", []byte(s))
		//labelNameEntries, err := queryIndex(ctx, iqs, ic)
		//if err != nil {
		//	log.Fatal("Failed to query index: ", err)
		//}
		//
		//var result chunk.UniqueStrings
		//result.Add(model.MetricNameLabel)
		//for _, entry := range labelNameEntries {
		//	lbs := []string{}
		//	err := jsoniter.ConfigFastest.Unmarshal(entry.Value, &lbs)
		//	if err != nil {
		//		log.Fatal("Failed to unmarshal label name:", err)
		//	}
		//	result.Add(lbs...)
		//}
		//labelNames := result.Strings()
		//for _, l := range labelNames {
		//	ln := model.LabelName(l)
		//	if _, ok := labelMap[ln]; !ok {
		//		//First time we've seen this label, load all the label values for it.
		//		labelMap[ln] = map[string]model.LabelValue{}
		//		iqs, err = sc.GetReadQueriesForMetricLabel(from, through, "29", nameLabelMatcher.Value, l)
		//		lvs, err := queryIndex(ctx, iqs, ic)
		//		if err != nil {
		//			log.Fatal("Failed to query index: ", err)
		//		}
		//		for _, lv := range lvs {
		//			seriesId, labelValue, _, err := parseChunkTimeRangeValue(lv.RangeValue, lv.Value)
		//			if err != nil {
		//				log.Fatal("Failed to parse label value lookup: ", err)
		//			}
		//			labelMap[ln][seriesId] = labelValue
		//		}
		//	}
		//	stream.labels[ln] = labelMap[ln][s]
		//}
		//log.Println("Found Labels in ", time.Now().Sub(start))
		//log.Println(stream)
		//streams = append(streams, stream)
		//
		//if len(streams) == 100 {
		//	break
		//}
	}
	log.Printf("Found %v Chunks in %v\n", len(chunks), time.Now().Sub(start))
	downloadChunks(config, ctx, chunks, cc)
}

func downloadChunks(config Config, ctx context.Context, chunks []string, cc chunk.Client) {
	for _, chk := range chunks {
		c, err := chunk.ParseExternalKey("fake", chk)
		if err != nil {
			log.Fatal("Failed to parse chunk key:", err)
		}
		_, err = cc.GetChunks(ctx, []chunk.Chunk{c})
		if err != nil {
			log.Printf("Failed to fetch chunk %v, err: %v\n", chk, err)
			if errors.Cause(err) == chunk.ErrStorageObjectNotFound {
				log.Println("Object was missing, adding an empty chunk")
				// Need to create an empty Loki memchunk to put inside the current shell of a chunk
				enc, err := chunkenc.ParseEncoding(config.Ingester.ChunkEncoding)
				if err != nil {
					log.Fatal("Failed to parse chunk encoding from config:", err)
				}
				mc := chunkenc.NewMemChunk(enc, config.Ingester.BlockSize, config.Ingester.TargetChunkSize)
				// Wrap the memchunk with our facade.
				c.Data = chunkenc.NewFacade(mc, config.Ingester.BlockSize, config.Ingester.TargetChunkSize)
				err = cc.PutChunks(ctx, []chunk.Chunk{c})
				if err != nil {
					log.Println("Failed to add chunk: $v", err)
				}
			}
		}
	}
}

func analyzeStreams(ctx context.Context, streams []stream, cc chunk.Client) {
	full := []string{}
	maxAge := []string{}
	idle := []stream{}

	for i, stream := range streams {
		if len(stream.chunks) > 1 {
			full = append(full, fmt.Sprintf("Stream %v had sufficent volume to fill %v chunks\n", stream.labels, len(stream.chunks)))
			continue
		}

		chk, err := chunk.ParseExternalKey("fake", stream.chunks[0])
		if err != nil {
			log.Fatal("Failed to parse chunk key:", err)
		}

		fcs, err := cc.GetChunks(ctx, []chunk.Chunk{chk})
		if err != nil {
			log.Println("Failed to fetch chunk:", err)
			continue
		}

		fc := fcs[0]
		if fc.Through.Sub(fc.From) >= time.Minute*60 {
			enc, err := fc.Encoded()
			if err != nil {
				log.Println("Error encoding chunk:", err)
				enc = []byte{}
			}
			maxAge = append(maxAge, fmt.Sprintf("Stream %v was flushed because it aged out, size: %v\n", stream, len(enc)))
		}

		idle = append(idle, streams[i])
	}
	fmt.Printf("%v/%v Streams filled multiple chunks:\n", len(full), len(streams))
	for _, s := range full {
		fmt.Printf("  %v", s)
	}

	fmt.Printf("\n\n")
	fmt.Printf("%v/%v Streams were filling but hit maxAge:\n", len(maxAge), len(streams))
	for _, s := range maxAge {
		fmt.Printf("  %v", s)
	}

	fmt.Printf("\n\n")
	fmt.Printf("%v/%v Streams flushed for being idle:\n", len(idle), len(streams))

	type labelStat struct {
		count int
		vals  map[model.LabelValue]int
	}
	labels := map[model.LabelName]*labelStat{}
	for _, s := range idle {
		for k, v := range s.labels {
			//Check if we've seen the label before
			if lStat, ok := labels[k]; ok {
				// Seen the label, increment it
				labels[k].count = labels[k].count + 1
				//Check to see if we've seen teh value before
				if _, ok := lStat.vals[v]; ok {
					//Seen the value, increment it
					labels[k].vals[v] = labels[k].vals[v] + 1
				} else {
					//Haven't seen the value add it
					labels[k].vals[v] = 1
				}
			} else {
				//We haven't seen the label add it
				labels[k] = &labelStat{
					count: 1,
					vals:  map[model.LabelValue]int{v: 1},
				}
			}
		}
	}
	lNames := []model.LabelName{}
	for ln := range labels {
		lNames = append(lNames, ln)
	}
	sort.SliceStable(lNames, func(i, j int) bool {
		return labels[lNames[i]].count > labels[lNames[j]].count
	})
	fmt.Printf("\n\n")
	fmt.Println("The following Labels were found in this quantity:")
	for _, l := range lNames {
		fmt.Printf("  %v: %v, val count: %v\n", l, labels[l].count, len(labels[l].vals))
	}

}

func queryIndex(ctx context.Context, iqs []chunk.IndexQuery, ic chunk.IndexClient) ([]chunk.IndexEntry, error) {
	var lock sync.Mutex
	var entries []chunk.IndexEntry
	err := ic.QueryPages(ctx, iqs, func(query chunk.IndexQuery, resp chunk.ReadBatch) bool {
		iter := resp.Iterator()
		lock.Lock()
		for iter.Next() {
			entries = append(entries, chunk.IndexEntry{
				TableName:  query.TableName,
				HashValue:  query.HashValue,
				RangeValue: iter.RangeValue(),
				Value:      iter.Value(),
			})
		}
		lock.Unlock()
		return true
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func parseIndexEntries(ctx context.Context, entries []chunk.IndexEntry, matcher *labels.Matcher) ([]string, error) {
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		chunkKey, labelValue, _, err := parseChunkTimeRangeValue(entry.RangeValue, entry.Value)
		if err != nil {
			return nil, err
		}

		if matcher != nil && !matcher.Matches(string(labelValue)) {
			continue
		}
		result = append(result, chunkKey)
	}
	// Return ids sorted and deduped because they will be merged with other sets.
	sort.Strings(result)
	result = uniqueStrings(result)
	return result, nil
}

func uniqueStrings(cs []string) []string {
	if len(cs) == 0 {
		return []string{}
	}

	result := make([]string, 1, len(cs))
	result[0] = cs[0]
	i, j := 0, 1
	for j < len(cs) {
		if result[i] == cs[j] {
			j++
			continue
		}
		result = append(result, cs[j])
		i++
		j++
	}
	return result
}

// parseChunkTimeRangeValue returns the chunkID and labelValue for chunk time
// range values.
func parseChunkTimeRangeValue(rangeValue []byte, value []byte) (
	chunkID string, labelValue model.LabelValue, isSeriesID bool, err error,
) {
	components := decodeRangeKey(rangeValue)

	switch {
	case len(components) < 3:
		err = errors.Errorf("invalid chunk time range value: %x", rangeValue)
		return

	// v1 & v2 schema had three components - label name, label value and chunk ID.
	// No version number.
	case len(components) == 3:
		chunkID = string(components[2])
		labelValue = model.LabelValue(components[1])
		return

	case len(components[3]) == 1:
		switch components[3][0] {
		// v3 schema had four components - label name, label value, chunk ID and version.
		// "version" is 1 and label value is base64 encoded.
		// (older code wrote "version" as 1, not '1')
		case chunkTimeRangeKeyV1a, chunkTimeRangeKeyV1:
			chunkID = string(components[2])
			labelValue, err = decodeBase64Value(components[1])
			return

		// v4 schema wrote v3 range keys and a new range key - version 2,
		// with four components - <empty>, <empty>, chunk ID and version.
		case chunkTimeRangeKeyV2:
			chunkID = string(components[2])
			return

		// v5 schema version 3 range key is chunk end time, <empty>, chunk ID, version
		case chunkTimeRangeKeyV3:
			chunkID = string(components[2])
			return

		// v5 schema version 4 range key is chunk end time, label value, chunk ID, version
		case chunkTimeRangeKeyV4:
			chunkID = string(components[2])
			labelValue, err = decodeBase64Value(components[1])
			return

		// v6 schema added version 5 range keys, which have the label value written in
		// to the value, not the range key. So they are [chunk end time, <empty>, chunk ID, version].
		case chunkTimeRangeKeyV5:
			chunkID = string(components[2])
			labelValue = model.LabelValue(value)
			return

		// v9 schema actually return series IDs
		case seriesRangeKeyV1:
			chunkID = string(components[0])
			isSeriesID = true
			return

		case labelSeriesRangeKeyV1:
			chunkID = string(components[1])
			labelValue = model.LabelValue(value)
			isSeriesID = true
			return
		case labelNamesRangeKeyV1:

		}

	}
	err = fmt.Errorf("unrecognised chunkTimeRangeKey version: %q", string(components[3]))
	return
}

func decodeRangeKey(value []byte) [][]byte {
	components := make([][]byte, 0, 5)
	i, j := 0, 0
	for j < len(value) {
		if value[j] != 0 {
			j++
			continue
		}
		components = append(components, value[i:j])
		j++
		i = j
	}
	return components
}

func decodeBase64Value(bs []byte) (model.LabelValue, error) {
	decodedLen := base64.RawStdEncoding.DecodedLen(len(bs))
	decoded := make([]byte, decodedLen)
	if _, err := base64.RawStdEncoding.Decode(decoded, bs); err != nil {
		return "", err
	}
	return model.LabelValue(decoded), nil
}
