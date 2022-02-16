package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"unicode"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/validation"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/weaveworks/common/logging"
)

var (
	yamlFieldNameParser   = regexp.MustCompile("^[^,]+")
	yamlFieldInlineParser = regexp.MustCompile("^[^,]*,inline$")
)

var excludedFlags = map[string]struct{}{"printversion": {}, "verifyconfig": {}, "printconfig": {}, "logconfig": {}, "configfile": {}, "configexpandenv": {}, "instance_id": {}}

type configBlock struct {
	name          string
	desc          string
	entries       []*configEntry
	flagsPrefix   string
	flagsPrefixes []string
}

func (b *configBlock) Add(entry *configEntry) {
	b.entries = append(b.entries, entry)
}

type configEntry struct {
	kind     string
	name     string
	required bool

	// In case the kind is "block"
	block     *configBlock
	blockDesc string
	root      bool

	// In case the kind is "field"
	fieldFlag    string
	fieldDesc    string
	fieldType    string
	fieldDefault string
}

type rootBlock struct {
	name       string
	desc       string
	structType reflect.Type
}

func parseFlags(cfg flagext.Registerer) map[uintptr]*flag.Flag {
	fs := flag.NewFlagSet("docs", flag.PanicOnError)
	cfg.RegisterFlags(fs)

	flags := map[uintptr]*flag.Flag{}
	fs.VisitAll(func(f *flag.Flag) {
		// Skip deprecated flags
		if f.Value.String() == "deprecated" {
			return
		}

		ptr := reflect.ValueOf(f.Value).Pointer()
		flags[ptr] = f
	})

	return flags
}

// resolveStruct resolves higher level types (slices and pointers) to an
// underlying struct when applicable.
func resolveStruct(t reflect.Type, v reflect.Value) (reflect.Type, interface{}) {
	switch t.Kind() {
	case reflect.Struct:
		return t, v.Addr().Interface()
	case reflect.Slice, reflect.Ptr:
		elem := t.Elem()
		// continue if the slice or pointer has underlying struct type.
		if elem.Kind() == reflect.Struct {
			return elem, reflect.New(elem).Interface()
		}
	}

	return nil, nil
}

func parseConfig(block *configBlock, cfg interface{}, flags map[uintptr]*flag.Flag) ([]*configBlock, error) {
	blocks := []*configBlock{}

	// If the input block is nil it means we're generating the doc for the top-level block
	if block == nil {
		block = &configBlock{}
		blocks = append(blocks, block)
	}

	// The input config is expected to be addressable.
	if reflect.TypeOf(cfg).Kind() != reflect.Ptr {
		t := reflect.TypeOf(cfg)
		return nil, fmt.Errorf("%s is a %s while a %s is expected", t, t.Kind(), reflect.Ptr)
	}

	// The input config is expected to be a pointer to struct.
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s is a %s while a %s is expected", v, v.Kind(), reflect.Struct)
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.FieldByIndex(field.Index)

		// Skip fields explicitly marked as "hidden" in the doc
		if isFieldHidden(field) {
			continue
		}

		// Skip fields not exported via yaml (unless they're inline)
		fieldName := getFieldName(field)
		if fieldName == "" && !isFieldInline(field) {
			continue
		}

		// Skip fields included in the excluded flags
		if _, ok := excludedFlags[fieldName]; ok {
			continue
		}

		// Skip field types which are non configurable
		if field.Type.Kind() == reflect.Func {
			continue
		}

		// Skip deprecated fields we're still keeping for backward compatibility
		// reasons (by convention we prefix them by UnusedFlag)
		if strings.HasPrefix(field.Name, "UnusedFlag") {
			continue
		}

		// Handle custom fields in vendored libs upon which we have no control.
		fieldEntry, err := getCustomFieldEntry(field, fieldValue, flags)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get custom field entry")
		}
		if fieldEntry != nil {
			block.Add(fieldEntry)
			continue
		}

		structField, structIntf := resolveStruct(field.Type, fieldValue)
		if structField != nil {

			// Check whether the sub-block is a root config block
			rootName, rootDesc, isRoot := isRootBlock(structField)

			// Since we're going to recursively iterate, we need to create a new sub
			// block and pass it to the doc generation function.
			var subBlock *configBlock

			if !isFieldInline(field) {
				var blockName string
				var blockDesc string

				if isRoot {
					blockName = rootName
					blockDesc = rootDesc
				} else {
					blockName = fieldName
					blockDesc = getFieldDescription(field, "")
				}

				subBlock = &configBlock{
					name: blockName,
					desc: blockDesc,
				}

				block.Add(&configEntry{
					kind:      "block",
					name:      fieldName,
					required:  isFieldRequired(field),
					block:     subBlock,
					blockDesc: blockDesc,
					root:      isRoot,
				})

				if isRoot {
					blocks = append(blocks, subBlock)
				}
			} else {
				subBlock = block
			}

			// Recursively generate the doc for the sub-block
			otherBlocks, err := parseConfig(subBlock, structIntf, flags)
			if err != nil {
				return nil, errors.Wrapf(err, "error parsing subBlock %s", subBlock.name)
			}

			blocks = append(blocks, otherBlocks...)
			continue
		}

		fieldType, err := getFieldType(field.Type)
		if err != nil {
			return nil, errors.Wrapf(err, "config=%s.%s", t.PkgPath(), t.Name())
		}

		entry := newFieldEntry(field, fieldValue, flags, fieldType)
		if entry != nil {
			block.Add(entry)
		}
	}

	return blocks, nil
}

func getFieldName(field reflect.StructField) string {
	name := field.Name
	tag := field.Tag.Get("yaml")

	// If the tag is not specified, then an exported field can be
	// configured via the field name (lowercase), while an unexported
	// field can't be configured.
	if tag == "" {
		if unicode.IsLower(rune(name[0])) {
			return ""
		}

		return strings.ToLower(name)
	}

	// Parse the field name
	fieldName := yamlFieldNameParser.FindString(tag)
	if fieldName == "-" {
		return ""
	}

	return fieldName
}

func getFieldType(t reflect.Type) (string, error) {
	// Handle custom data types used in the config
	switch t.String() {
	case "*url.URL":
		return "url", nil
	case "time.Duration":
		return "duration", nil
	case "model.Duration":
		return "duration", nil
	// flagext module
	case "flagext.StringSliceCSV":
		return "string", nil
	case "flagext.CIDRSliceCSV":
		return "string", nil
	case "flagext.Secret":
		return "string", nil
	case "flagext.URLValue":
		return "url", nil
	// logging module
	case "logging.Format":
		return "string", nil
	case "logging.Level":
		return "string", nil
	case "*util.RelabelConfig":
		return "relabel_config...", nil
	case "*config.RemoteWriteConfig":
		return "remote_write_config...", nil
	}

	// Fallback to auto-detection of built-in data types
	switch t.Kind() {
	case reflect.Bool:
		return "boolean", nil

	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		return "int", nil

	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return "float", nil

	case reflect.String:
		return "string", nil

	case reflect.Slice:
		// Get the type of elements
		elemType, err := getFieldType(t.Elem())
		if err != nil {
			return "", err
		}

		return "list of " + elemType, nil

	case reflect.Map:
		return fmt.Sprintf("map of %s to %s", t.Key(), t.Elem().String()), nil

	default:
		return "", fmt.Errorf("unsupported data type %s", t.Kind())
	}
}

func getFieldFlag(field reflect.StructField, fieldValue reflect.Value, flags map[uintptr]*flag.Flag) *flag.Flag {
	if isAbsentInCLI(field) {
		return nil
	}
	fieldPtr := fieldValue.Addr().Pointer()
	fieldFlag, ok := flags[fieldPtr]
	if !ok {
		fmt.Fprintf(os.Stderr, "[WARNING]: unable to find CLI flag for '%s' config entry\n", field.Name)
	}

	return fieldFlag
}

func newFieldEntry(field reflect.StructField, fieldValue reflect.Value, flags map[uintptr]*flag.Flag, fieldType string) *configEntry {
	if fieldFlag := getFieldFlag(field, fieldValue, flags); fieldFlag != nil {
		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldType:    fieldType,
			fieldFlag:    fieldFlag.Name,
			fieldDesc:    getFieldDescription(field, fieldFlag.Usage),
			fieldDefault: fieldFlag.DefValue,
		}
	}
	fieldDescription := getFieldDescription(field, "")
	if fieldDescription == "" {
		return nil
	}
	return &configEntry{
		kind:      "field",
		name:      getFieldName(field),
		required:  isFieldRequired(field),
		fieldType: fieldType,
		fieldDesc: fieldDescription,
	}
}

func getCustomFieldEntry(field reflect.StructField, fieldValue reflect.Value, flags map[uintptr]*flag.Flag) (*configEntry, error) {
	for _, t := range []reflect.Type{
		reflect.TypeOf(url.URL{}),
		reflect.TypeOf(logging.Level{}),
		reflect.TypeOf(logging.Format{}),
		reflect.TypeOf(flagext.URLValue{}),
		reflect.TypeOf(flagext.Secret{}),
		reflect.TypeOf(flagext.Time{}),
		reflect.TypeOf(model.Duration(0)),
	} {
		if field.Type == t {
			fieldType, err := getFieldType(field.Type)
			if err != nil {
				return nil, fmt.Errorf("could not derive type for custom field '%s.%s'", field.PkgPath, field.Name)
			}
			return newFieldEntry(field, fieldValue, flags, fieldType), nil
		}
	}

	// TODO (@chaudum):
	// Support slices of structs by creating a separate config block for them
	// and referencing them as 'list of <config_block>' in the field's type.
	// Slices of structs cannot be provided as CLI arguments.
	if field.Type == reflect.SliceOf(reflect.TypeOf(chunk.PeriodConfig{})) {
		doc := `The 'period_config' block configures what index schemas should be used for from specific time periods.
See https://grafana.com/docs/loki/latest/configuration/#period_config`
		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    "",
			fieldDesc:    doc,
			fieldType:    "array",
			fieldDefault: "none",
		}, nil
	}

	if field.Type == reflect.SliceOf(reflect.TypeOf(validation.StreamRetention{})) {
		doc := `Per-stream retention to apply, if the retention is enable on the compactor side.
Example:
retention_stream:
- selector: '{namespace="dev"}'
  priority: 1
  period: 24h
- selector: '{container="nginx"}'
  priority: 1
  period: 744h
Selector is a Prometheus labels matchers that will apply the 'period' retention only if the stream is matching. In case multiple stream are matching, the highest priority will be picked. If no rule is matched the 'retention_period' is used.`
		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    "",
			fieldDesc:    doc,
			fieldType:    "array",
			fieldDefault: "none",
		}, nil
	}

	if field.Type == reflect.TypeOf(config.RemoteWriteConfig{}) {
		doc := `Remote-write client configuration to send rule samples to a Prometheus remote-write endpoint.
See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write`
		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    "",
			fieldDesc:    doc,
			fieldType:    "object",
			fieldDefault: "none",
		}, nil
	}

	return nil, nil
}

func isFieldHidden(f reflect.StructField) bool {
	return getDocTagFlag(f, "hidden")
}

func isAbsentInCLI(f reflect.StructField) bool {
	return getDocTagFlag(f, "nocli")
}

func isFieldRequired(f reflect.StructField) bool {
	return getDocTagFlag(f, "required")
}

func isFieldInline(f reflect.StructField) bool {
	return yamlFieldInlineParser.MatchString(f.Tag.Get("yaml"))
}

func getFieldDescription(f reflect.StructField, fallback string) string {
	if desc := getDocTagValue(f, "description"); desc != "" {
		return desc
	}

	return fallback
}

func isRootBlock(t reflect.Type) (string, string, bool) {
	for _, rootBlock := range rootBlocks {
		if t == rootBlock.structType {
			return rootBlock.name, rootBlock.desc, true
		}
	}

	return "", "", false
}

func getDocTagFlag(f reflect.StructField, name string) bool {
	cfg := parseDocTag(f)
	_, ok := cfg[name]
	return ok
}

func getDocTagValue(f reflect.StructField, name string) string {
	cfg := parseDocTag(f)
	return cfg[name]
}

func parseDocTag(f reflect.StructField) map[string]string {
	cfg := map[string]string{}
	tag := f.Tag.Get("doc")

	if tag == "" {
		return cfg
	}

	for _, entry := range strings.Split(tag, "|") {
		parts := strings.SplitN(entry, "=", 2)

		switch len(parts) {
		case 1:
			cfg[parts[0]] = ""
		case 2:
			cfg[parts[0]] = parts[1]
		}
	}

	return cfg
}
