package window

import (
	"log"
	"sort"
	"time"
)

// Window represents a sliding window of Elements. Elements inserted into the
// Window are placed in the appropriate bucket. Buckets are subsequently "flushed"
// and written out to the Out channel.
//
// The sliding window provides the following guarantees:
// 1. No elements outside of the sliding window will be returned (or stored)
// 2. Elements returned will always be ordered by Element Timestamp
// 3. Elements will be pushed on the Out channel at regular intervals as dictated by the total Window Size and the number of Slots
type Window struct {
	Slots      []ElementBucket
	Out        chan ElementBucket
	windowSize time.Duration
	oldest     time.Time
	ticker     *time.Ticker
}

type Element struct {
	Timestamp time.Time
	Value     interface{}
}

// ElementBucket is an alias for a Slice of *Elements
type ElementBucket []*Element

// New Window Constructor
func New(slotCount int, windowSize time.Duration) *Window {
	slotWidth := windowSize / time.Duration(slotCount)
	ticker := time.NewTicker(slotWidth)
	outChan := make(chan ElementBucket)

	w := &Window{
		Slots:      make([]ElementBucket, slotCount),
		Out:        outChan,
		windowSize: windowSize,
		oldest:     time.Now().UTC().Add(-(windowSize / 2)),
		//oldest: time.Now().UTC(),
		ticker: ticker,
	}

	go func() {
		for range ticker.C {
			outChan <- w.pop()
		}
	}()

	return w
}

// Insert Element into Window
func (w *Window) Insert(e *Element) error {

	// Discard if outside of window
	if e.Timestamp.Before(w.oldest) || e.Timestamp.After(w.oldest.Add(w.windowSize)) {
		log.Printf("Discarding message with timestamp %s (oldest is %s).", e.Timestamp.UTC().String(), w.oldest.UTC().String())
		return nil
	}

	// Delta from oldest
	delta := e.Timestamp.Sub(w.oldest)

	// Normalize to bucket width
	d := float64(delta.Nanoseconds()) / float64(w.windowSize.Nanoseconds())

	// Map to correct bucket
	bucket := int(d * float64(len(w.Slots)))

	// Initialize if bucket doesn't already have empty slice
	if len(w.Slots[bucket]) == 0 {
		w.Slots[bucket] = ElementBucket{}
	}
	w.Slots[bucket] = append(w.Slots[bucket], e)

	return nil
}

func (w *Window) pop() ElementBucket {

	// Pop head of slice
	var head ElementBucket
	head, w.Slots = w.Slots[0], w.Slots[1:]

	// Add empty slice at the end
	w.Slots = append(w.Slots, ElementBucket{})

	// Reset oldest
	w.oldest = time.Now().UTC().Add(-(w.windowSize / 2))

	// Sort head
	sort.Sort(head)

	return head
}

// Peek returns the Element that would be popped next
func (w *Window) Peek() ElementBucket {
	head := w.Slots[0]
	sort.Sort(head)

	return head
}

// Close the Window
func (w *Window) Close() {
	w.ticker.Stop()
}

func (e ElementBucket) Len() int {
	return len(e)
}

func (e ElementBucket) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e ElementBucket) Less(i, j int) bool {
	return e[i].Timestamp.Before(e[j].Timestamp)
}
