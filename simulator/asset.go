package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Telemetry is the wire format published to Kafka. The ingest service validates
// the same shape (ingest/app/models.py), so changes here must land in both.
type Telemetry struct {
	AssetID    string    `json:"asset_id"`
	Seq        int64     `json:"seq"`
	TS         time.Time `json:"ts"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	AltKm      float64   `json:"alt_km"`
	BatteryPct float64   `json:"battery_pct"`
	TempC      float64   `json:"temp_c"`
	SignalDB   float64   `json:"signal_db"`
	Status     string    `json:"status"`
}

type Fault int

const (
	FaultNone Fault = iota
	FaultBatteryDrain
	FaultOverheat
	FaultCommsLoss
)

func (f Fault) String() string {
	switch f {
	case FaultBatteryDrain:
		return "battery_drain"
	case FaultOverheat:
		return "overheat"
	case FaultCommsLoss:
		return "comms_loss"
	default:
		return "none"
	}
}

const (
	earthRotationRadPerSec = 2 * math.Pi / 86164.0
	deg                    = math.Pi / 180
)

// Asset is one simulated satellite on a circular orbit. Orbital motion is
// sped up by timeScale so a demo shows visible movement within minutes.
type Asset struct {
	ID          string
	inclination float64 // radians
	raan        float64 // radians
	phase       float64 // argument of latitude, radians
	periodSec   float64
	altKm       float64

	seq     int64
	battery float64
	tempC   float64

	fault          Fault
	faultTicksLeft int
}

func NewAsset(index int, rng *rand.Rand) *Asset {
	// Spread assets across 12 orbital planes, evenly phased within each plane.
	const planes = 12
	plane := index % planes
	slot := index / planes
	return &Asset{
		ID:          fmt.Sprintf("SAT-%05d", index),
		inclination: 53 * deg,
		raan:        float64(plane) * (2 * math.Pi / planes),
		phase:       float64(slot)*0.37 + rng.Float64()*0.1,
		periodSec:   95 * 60,
		altKm:       540 + rng.Float64()*20,
		battery:     70 + rng.Float64()*30,
		tempC:       15 + rng.Float64()*10,
	}
}

// InjectFault forces a fault for the given number of ticks.
func (a *Asset) InjectFault(f Fault, ticks int) {
	a.fault = f
	a.faultTicksLeft = ticks
}

func (a *Asset) Fault() Fault { return a.fault }

// Step advances the asset by dt (already time-scaled) and returns its
// telemetry. ok is false when the asset is in comms loss: the sequence
// number still advances, so the consumer can detect the gap.
func (a *Asset) Step(now time.Time, dt time.Duration, simSeconds float64, rng *rand.Rand) (t Telemetry, ok bool) {
	a.seq++
	suppressed := a.fault == FaultCommsLoss
	a.phase = math.Mod(a.phase+2*math.Pi*dt.Seconds()/a.periodSec, 2*math.Pi)

	lat := math.Asin(math.Sin(a.inclination) * math.Sin(a.phase))
	lon := math.Atan2(math.Cos(a.inclination)*math.Sin(a.phase), math.Cos(a.phase)) +
		a.raan - earthRotationRadPerSec*simSeconds
	lon = wrapPi(lon)

	// Crude eclipse model: half of every orbit charges, the rest drains.
	inSun := math.Sin(a.phase) > -0.3
	if inSun {
		a.battery += 0.08 * dt.Seconds()
		a.tempC += (25 - a.tempC) * 0.01 * dt.Seconds()
	} else {
		a.battery -= 0.05 * dt.Seconds()
		a.tempC += (5 - a.tempC) * 0.01 * dt.Seconds()
	}

	switch a.fault {
	case FaultBatteryDrain:
		a.battery -= 0.6 * dt.Seconds()
	case FaultOverheat:
		a.tempC += 0.8 * dt.Seconds()
	}
	a.battery = clamp(a.battery, 0, 100)

	if a.fault != FaultNone {
		a.faultTicksLeft--
		if a.faultTicksLeft <= 0 {
			a.fault = FaultNone
		}
	}

	status := "nominal"
	if a.battery < 15 || a.tempC > 60 {
		status = "safe_mode"
	}

	t = Telemetry{
		AssetID:    a.ID,
		Seq:        a.seq,
		TS:         now.UTC(),
		Lat:        round(lat/deg, 4),
		Lon:        round(lon/deg, 4),
		AltKm:      round(a.altKm+rng.NormFloat64()*0.2, 2),
		BatteryPct: round(a.battery, 2),
		TempC:      round(a.tempC+rng.NormFloat64()*0.3, 2),
		SignalDB:   round(-70+rng.NormFloat64()*3, 1),
		Status:     status,
	}
	return t, !suppressed
}

func wrapPi(x float64) float64 {
	x = math.Mod(x+math.Pi, 2*math.Pi)
	if x < 0 {
		x += 2 * math.Pi
	}
	return x - math.Pi
}

func clamp(x, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, x)) }

func round(x float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(x*p) / p
}
