package main

import (
	"math/rand"
	"testing"
	"time"
)

func newTestAsset() (*Asset, *rand.Rand) {
	rng := rand.New(rand.NewSource(1))
	return NewAsset(7, rng), rng
}

func TestStepStaysInPhysicalBounds(t *testing.T) {
	a, rng := newTestAsset()
	now := time.Unix(0, 0)
	for i := 0; i < 5000; i++ {
		now = now.Add(time.Second)
		tel, _ := a.Step(now, 20*time.Second, float64(i)*20, rng)
		if tel.Lat < -53.01 || tel.Lat > 53.01 {
			t.Fatalf("lat %v outside inclination band", tel.Lat)
		}
		if tel.Lon < -180 || tel.Lon > 180 {
			t.Fatalf("lon %v out of range", tel.Lon)
		}
		if tel.BatteryPct < 0 || tel.BatteryPct > 100 {
			t.Fatalf("battery %v out of range", tel.BatteryPct)
		}
	}
}

func TestSeqIncrementsEvenDuringCommsLoss(t *testing.T) {
	a, rng := newTestAsset()
	a.InjectFault(FaultCommsLoss, 3)
	now := time.Unix(0, 0)

	var published []int64
	for i := 0; i < 5; i++ {
		tel, ok := a.Step(now, time.Second, 0, rng)
		if ok {
			published = append(published, tel.Seq)
		}
	}
	// Ticks 1-3 are suppressed; the consumer should see 4 then 5 and infer a gap.
	if len(published) != 2 || published[0] != 4 || published[1] != 5 {
		t.Fatalf("published seqs = %v, want [4 5]", published)
	}
}

func TestBatteryDrainFaultEntersSafeMode(t *testing.T) {
	a, rng := newTestAsset()
	a.InjectFault(FaultBatteryDrain, 1_000_000)
	var tel Telemetry
	for i := 0; i < 500; i++ {
		tel, _ = a.Step(time.Unix(int64(i), 0), 2*time.Second, 0, rng)
	}
	if tel.Status != "safe_mode" {
		t.Fatalf("status = %q after sustained drain (battery %.1f), want safe_mode", tel.Status, tel.BatteryPct)
	}
}

func TestFaultClearsAfterTicks(t *testing.T) {
	a, rng := newTestAsset()
	a.InjectFault(FaultOverheat, 2)
	a.Step(time.Unix(0, 0), time.Second, 0, rng)
	a.Step(time.Unix(1, 0), time.Second, 0, rng)
	if a.Fault() != FaultNone {
		t.Fatalf("fault = %v, want none", a.Fault())
	}
}
