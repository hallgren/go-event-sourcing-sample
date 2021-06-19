package eventsourcing_test

import (
	"encoding/json"
	"encoding/xml"
	"testing"

	"github.com/hallgren/eventsourcing"
	"github.com/hallgren/eventsourcing/eventstore/memory"
	memsnap "github.com/hallgren/eventsourcing/snapshotstore/memory"
)

func TestSaveAndGetAggregate(t *testing.T) {
	repo := eventsourcing.NewRepository(memory.Create(), nil)

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	err = repo.Save(person)
	if err != nil {
		t.Fatal("could not save aggregate")
	}

	// make sure the global version is set to 1
	if person.GlobalVersion() != 1 {
		t.Fatalf("global version is: %d expected: 1", person.GlobalVersion())
	}

	twin := Person{}
	err = repo.Get(person.ID(), &twin)
	if err != nil {
		t.Fatal("could not get aggregate")
	}

	// Check internal aggregate version
	if person.Version() != twin.Version() {
		t.Fatalf("Wrong version org %q copy %q", person.Version(), twin.Version())
	}

	// Check person Name
	if person.Name != twin.Name {
		t.Fatalf("Wrong Name org %q copy %q", person.Name, twin.Name)
	}
}

func TestGetNoneExistingAggregate(t *testing.T) {
	repo := eventsourcing.NewRepository(memory.Create(), nil)

	p := Person{}
	err := repo.Get("none_existing", &p)
	if err != eventsourcing.ErrAggregateNotFound {
		t.Fatal("could not get aggregate")
	}
}

func TestSaveAndGetAggregateSnapshotAndEvents(t *testing.T) {
	ser := eventsourcing.NewSerializer(xml.Marshal, xml.Unmarshal)
	repo := eventsourcing.NewRepository(memory.Create(), eventsourcing.SnapshotNew(memsnap.New(), *ser))

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	err = repo.Save(person)
	if err != nil {
		t.Fatal("could not save aggregate")
	}

	// save person to snapshot store
	err = repo.SaveSnapshot(person)
	if err != nil {
		t.Fatal(err)
	}
	person.GrowOlder()
	repo.Save(person)
	twin := Person{}
	err = repo.Get(person.ID(), &twin)
	if err != nil {
		t.Fatal("could not get aggregate")
	}

	// Check internal aggregate version
	if person.Version() != twin.Version() {
		t.Fatalf("Wrong version org %q copy %q", person.Version(), twin.Version())
	}

	// Check person Name
	if person.Name != twin.Name {
		t.Fatalf("Wrong Name org %q copy %q", person.Name, twin.Name)
	}
}

func TestSaveSnapshotWithUnsavedEvents(t *testing.T) {
	ser := eventsourcing.NewSerializer(json.Marshal, json.Unmarshal)
	repo := eventsourcing.NewRepository(memory.Create(), eventsourcing.SnapshotNew(memsnap.New(), *ser))

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	// save person to snapshot store
	err = repo.SaveSnapshot(person)
	if err == nil {
		t.Fatalf("should not save snapshot with unsaved events %v", err)
	}
}

func TestSaveSnapshotWithoutSnapshotStore(t *testing.T) {
	repo := eventsourcing.NewRepository(memory.Create(), nil)

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	// save person to snapshot store
	err = repo.SaveSnapshot(person)
	if err == nil {
		t.Fatal("should not save snapshot when there is no snapshot store")
	}
}

func TestSubscriptionAllEvent(t *testing.T) {
	counter := 0
	f := func(e eventsourcing.Event) {
		counter++
	}
	repo := eventsourcing.NewRepository(memory.Create(), nil)
	s, err := repo.SubscriberAll(f)
	if err != nil {
		t.Fatal(err)
	}
	s.Subscribe()
	defer s.Unsubscribe()

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	person.GrowOlder()
	person.GrowOlder()
	person.GrowOlder()

	err = repo.Save(person)
	if err != nil {
		t.Fatal("could not save aggregate")
	}
	if counter != 4 {
		t.Errorf("No global events was received from the stream, got %q", counter)
	}
}

func TestSubscriptionSpecificEvent(t *testing.T) {
	counter := 0
	f := func(e eventsourcing.Event) {
		counter++
	}
	repo := eventsourcing.NewRepository(memory.Create(), nil)
	s, err := repo.SubscriberSpecificEvent(f, &Born{}, &AgedOneYear{})
	if err != nil {
		t.Fatal(err)
	}
	s.Subscribe()
	defer s.Unsubscribe()

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	person.GrowOlder()
	person.GrowOlder()
	person.GrowOlder()

	err = repo.Save(person)
	if err != nil {
		t.Fatal("could not save aggregate")
	}
	if counter != 4 {
		t.Errorf("No global events was received from the stream, got %d", counter)
	}
}

func TestSubscriptionAggregateType(t *testing.T) {
	counter := 0
	f := func(e eventsourcing.Event) {
		counter++
	}
	repo := eventsourcing.NewRepository(memory.Create(), nil)
	s, err := repo.SubscriberAggregateType(f, &Person{})
	if err != nil {
		t.Fatal(err)
	}
	s.Subscribe()
	defer s.Unsubscribe()

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	person.GrowOlder()
	person.GrowOlder()
	person.GrowOlder()

	err = repo.Save(person)
	if err != nil {
		t.Fatal("could not save aggregate")
	}
	if counter != 4 {
		t.Errorf("No global events was received from the stream, got %q", counter)
	}
}

func TestSubscriptionSpecificAggregate(t *testing.T) {
	counter := 0
	f := func(e eventsourcing.Event) {
		counter++
	}
	repo := eventsourcing.NewRepository(memory.Create(), nil)

	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	s, err := repo.SubscriberSpecificAggregate(f, person)
	if err != nil {
		t.Fatal(err)
	}
	s.Subscribe()
	defer s.Unsubscribe()

	person.GrowOlder()
	person.GrowOlder()
	person.GrowOlder()

	err = repo.Save(person)
	if err != nil {
		t.Fatal("could not save aggregate")
	}
	if counter != 4 {
		t.Errorf("No global events was received from the stream, got %q", counter)
	}
}

func TestEventChainDoesNotHang(t *testing.T) {
	repo := eventsourcing.NewRepository(memory.Create(), nil)

	// eventChan can hold 5 events before it get full and blocks.
	eventChan := make(chan eventsourcing.Event, 5)
	doneChan := make(chan struct{})
	f := func(e eventsourcing.Event) {
		eventChan <- e
	}

	// for every AgedOnYear create a new person and make it grow one year older
	go func() {
		for e := range eventChan {
			switch e.Data.(type) {
			case *AgedOneYear:
				person, err := CreatePerson("kalle")
				if err != nil {
					t.Fatal(err)
				}
				person.GrowOlder()
				repo.Save(person)
			}
		}
		close(doneChan)
	}()

	// create the initial person and setup event subscription on the specific person events
	person, err := CreatePerson("kalle")
	if err != nil {
		t.Fatal(err)
	}
	s, err := repo.SubscriberSpecificAggregate(f, person)
	if err != nil {
		t.Fatal(err)
	}
	s.Subscribe()
	defer s.Unsubscribe()

	// subscribe to all events and filter out AgedOneYear
	ageCounter := 0
	s2, err := repo.SubscriberAll(func(e eventsourcing.Event) {
		switch e.Data.(type) {
		case *AgedOneYear:
			// will match three times on the initial person and one each on the resulting AgedOneYear event
			ageCounter++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	s2.Subscribe()
	defer s2.Unsubscribe()

	person.GrowOlder()
	person.GrowOlder()
	person.GrowOlder()

	err = repo.Save(person)
	if err != nil {
		t.Fatal("could not save aggregate")
	}
	close(eventChan)
	<-doneChan
	if ageCounter != 6 {
		t.Errorf("wrong number in ageCounter expected 6, got %v", ageCounter)
	}
}

func TestSubscriptionNoEventStream(t *testing.T) {
	repo := eventsourcing.NewRepositoryNoEventStream(memory.Create(), nil)

	counter := 0
	f := func(e eventsourcing.Event) {
		counter++
	}
	_, err := repo.SubscriberAll(f)
	if err != eventsourcing.ErrNoEventStream {
		t.Fatal("expected no event stream error")
	}
	_, err = repo.SubscriberAggregateType(f, nil)
	if err != eventsourcing.ErrNoEventStream {
		t.Fatal("expected no event stream error")
	}
	_, err = repo.SubscriberSpecificAggregate(f, nil)
	if err != eventsourcing.ErrNoEventStream {
		t.Fatal("expected no event stream error")
	}
	_, err = repo.SubscriberSpecificEvent(f, nil)
	if err != eventsourcing.ErrNoEventStream {
		t.Fatal("expected no event stream error")
	}

}
