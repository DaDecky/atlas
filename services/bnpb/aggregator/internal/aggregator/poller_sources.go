package aggregator

import (
	"net/http"
	"time"
)

// pollerSource holds what differs between the BMKG and PVMBG APIs, so the poller has no branch per source.
type pollerSource struct {
	path, idField, timeField string
	authorize                func(*http.Request, string)
	// warnings is true for BMKG, whose tsunami warnings revise stored earthquakes.
	warnings bool
	// policy is true for PVMBG, whose schema version selects the field names.
	policy    bool
	mapRecord func(o Object, warnings []Warning, policy SchemaPolicy, now time.Time) (HazardEvent, error)
}

var pollerSources = map[string]pollerSource{
	SourceBMKG: {
		path: "/seismic-events", idField: "event_id", timeField: "occurred_at", warnings: true,
		authorize: func(r *http.Request, key string) { r.Header.Set("X-BMKG-Key", key) },
		mapRecord: func(o Object, warnings []Warning, _ SchemaPolicy, now time.Time) (HazardEvent, error) {
			return MapSeismic(o, warnings, now)
		},
	},
	SourcePVMBG: {
		path: "/volcanic-reports", idField: "report_id", timeField: "reported_at", policy: true,
		authorize: func(r *http.Request, token string) { r.Header.Set("Authorization", "Bearer "+token) },
		mapRecord: func(o Object, _ []Warning, policy SchemaPolicy, now time.Time) (HazardEvent, error) {
			return policy.MapVolcanic(o, now)
		},
	},
}

// PolledSources are the sources that the Poller reads, in the order they start.
var PolledSources = []string{SourceBMKG, SourcePVMBG}

// schemaPolicy reads the policy file only for a source whose schema version selects its fields.
func (p *Poller) schemaPolicy(src pollerSource) (SchemaPolicy, error) {
	if !src.policy {
		return SchemaPolicy{}, nil
	}

	return ReadSchemaPolicy(p.PolicyFile)
}
