package analytics

import "time"

// Point is one daily aggregation point of a growth report.
type Point struct {
	Time time.Time `json:"time"`
	Rate float64   `json:"rate"`
}

// GrowthReport describes the message growth of one topic over a window.
type GrowthReport struct {
	Topic     string        `json:"topic"`
	Window    time.Duration `json:"window"`
	Points    []Point       `json:"points"`
	Delta     float64       `json:"delta"` // msgs/sec, first → last point
	Sparkline string        `json:"sparkline"`
}

// SkewReport describes the cluster-wide partition leadership distribution.
// Leaders maps broker id → partition count; a topic of "" means the report
// covers the whole cluster.
type SkewReport struct {
	Topic    string         `json:"topic"`
	Leaders  map[string]int `json:"leaders"`
	Ratio    float64        `json:"ratio"` // max / avg leaders per broker
	Balanced bool           `json:"balanced"`
}

// RetentionReport describes the retention posture of one topic.
type RetentionReport struct {
	Topic string `json:"topic"`
	// RetentionMS is the configured retention.ms; 0 means unknown or unset.
	RetentionMS time.Duration `json:"retention_ms"`
	// EstimateFillDays is how many days of data fit within retention.bytes
	// at the current byte rate; 0 when either side is unknown.
	EstimateFillDays float64 `json:"estimate_fill_days"`
	// OldestOffsetAge is the age of the oldest persisted data point.
	OldestOffsetAge time.Duration `json:"oldest_offset_age"`
	// AtRisk reports whether the byte-based fill estimate is shorter than
	// the time-based retention.
	AtRisk bool `json:"at_risk"`
}
