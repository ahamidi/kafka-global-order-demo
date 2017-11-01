package window

import (
	"log"
	"testing"
	"time"
)

func TestBucketOrder(t *testing.T) {
	w := New(10, 10*time.Second)
	defer w.Close()

	oldElement := &Element{
		Timestamp: time.Now().Add(-1 * 3 * time.Second),
		Value:     "this is an old message",
	}

	firstElement := &Element{
		Timestamp: time.Now(),
		Value:     "this is a current message",
	}

	futureElement := &Element{
		Timestamp: time.Now().Add(3 * time.Second),
		Value:     "this is an new message",
	}

	w.Insert(futureElement)
	w.Insert(firstElement)
	w.Insert(oldElement)

	e := <-w.Out
	if len(e) != 1 {
		t.Error("More than one element returned")
		printElements(e)
	}

	if e[0] != firstElement {
		t.Error("Expected current element, got ", e[0])
	}

	e = <-w.Out // Second bucket should be empty
	e = <-w.Out // Third bucket should be empty
	e = <-w.Out
	if e[0] != futureElement {
		t.Error("Expected new element, got ", e[0])
	}
}

func TestSortWithinBucket(t *testing.T) {
	w := New(10, 10*time.Second)
	defer w.Close()

	element1 := &Element{
		Timestamp: time.Now(),
		Value:     "this is a current message",
	}

	element2 := &Element{
		Timestamp: time.Now().Add(100 * time.Millisecond),
		Value:     "this is a current message",
	}

	element3 := &Element{
		Timestamp: time.Now().Add(200 * time.Millisecond),
		Value:     "this is a current message",
	}

	w.Insert(element2)
	w.Insert(element1)
	w.Insert(element3)

	e := <-w.Out
	if len(e) != 3 {
		t.Error("Expected 3 elements, got ", len(e))
		printElements(e)
	}

	if e[0] != element1 {
		t.Error("Expected element1, got ", e[0])
	}
}

func printElements(e []*Element) {
	for i, el := range e {
		if el != nil {
			log.Printf("e[%d]: %s", i, el)
		}
	}
}
