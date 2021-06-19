[![Build Status](https://travis-ci.org/hallgren/eventsourcing.svg?branch=master)](https://travis-ci.org/hallgren/eventsourcing)
[![Go Report Card](https://goreportcard.com/badge/github.com/hallgren/eventsourcing)](https://goreportcard.com/report/github.com/hallgren/eventsourcing)
[![codecov](https://codecov.io/gh/hallgren/eventsourcing/branch/master/graph/badge.svg)](https://codecov.io/gh/hallgren/eventsourcing)

# Overview

This package is an experiment to try to generialize [@jen20's](https://github.com/jen20) way of implementing event sourcing. You can find the original blog post [here](https://jen20.dev/post/event-sourcing-in-go/) and github repo [here](https://github.com/jen20/go-event-sourcing-sample).

## Event Sourcing

[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) is a technique to make it possible to capture all changes to an application state as a sequence of events.

### Aggregate Root

The *aggregate root* is the central point where events are bound. The aggregate struct needs to embed `eventsourcing.AggreateRoot` to get the aggregate behaviors.

Below, a *Person* aggregate where the Aggregate Root is embedded next to the `Name` and `Age` properties.

```go
type Person struct {
	eventsourcing.AggregateRoot
	Name string
	Age  int
}
```

The aggregate needs to implement the `Transition(event eventsourcing.Event)` function to fulfill the aggregate interface. This function define how events are transformed to build the aggregate state.

Example of the Transition function from the `Person` aggregate.

```go
// Transition the person state dependent on the events
func (person *Person) Transition(event eventsourcing.Event) {
        switch e := event.Data.(type) {
        case *Born:
                person.Age = 0
                person.Name = e.Name
        case *AgedOneYear:
                person.Age += 1
        }
}
```

In this example we can see that the `Born` event sets the `Person` property `Age` and `Name`, and that the `AgedOneYear` adds one year to the `Age` property. This makes the state of the aggregate flexible and could easily change in the future if required.

### Aggregate Event

An event is a clean struct with exported properties that contains the state of the event.

Example of two events from the `Person` aggregate.

```go
// Initial event
type Born struct {
        Name string
}

// Event that happens once a year
type AgedOneYear struct {}
```

When an aggregate is first created, an event is needed to initialize the state of the aggregate. No event, no aggregate. Below is an example of a constructor that returns the `Person` aggregate and inside it binds an event via the `TrackChange` function. It's possible to define rules that the aggregate must uphold before an event is created, in this case the person's name must not be blank.

```go
// CreatePerson constructor for Person
func CreatePerson(name string) (*Person, error) {
	if name == "" {
		return nil, errors.New("name can't be blank")
	}
	person := Person{}
	person.TrackChange(&person, &Born{Name: name})
	return &person, nil
}
```

When a person is created, more events could be created via functions on the `Person` aggregate. Below is the `GrowOlder` function which in turn triggers the event `AgedOneYear`. This event is tracked on the person aggregate.

```go
// GrowOlder command
func (person *Person) GrowOlder() {
	person.TrackChange(person, &AgedOneYear{})
}
```

Internally the `TrackChange` functions calls the `Transition` function on the aggregate to transform the aggregate based on the newly created event.

To bind meta data to events use the `TrackChangeWithMetaData` function.
  

The internal `Event` looks like this.

```go
type Event struct {
    // aggregate identifier 
    AggregateID string
    // the aggregate version when this event was created
    Version         Version
    // the global version is based on all events (this value is only set after the event is saved to the event store) 
    GlobalVersion   Version
    // aggregate type (Person in the example above)
    AggregateType   string
    // UTC time when the event was created  
    Timestamp       time.Time
    // the specific event data specified in the application (Born{}, AgedOneYear{})
    Data            interface{}
    // data that don´t belongs to the application state (could be correlation id or other request references)
    MetaData        map[string]interface{}
}
```

### Aggregate ID

The identifier on the aggregate is default set by a random generated string via the crypt/rand pkg. It is possible to change the default behaivior in two ways.

* Set a specific id on the aggregate via the SetID func.

```go
var id = "123"
person := Person{}
err := person.SetID(id)
```

* Change the id generator via the global eventsourcing.SetIDFunc function.

```go
var counter = 0
f := func() string {
	counter++
	return fmt.Sprint(counter)
}

eventsourcing.SetIDFunc(f)
```

## Repository

The repository is used to save and retrieve aggregates. The main functions are:

```go
// saves the events on the aggregate
Save(aggregate Aggregate) error

// retrieves and build an aggregate from events based on its identifier
Get(id string, aggregate Aggregate) error

// return count number of events in global order starting at the start position
GlobalEvents(start, count uint64) ([]Event, error) {
```

It is possible to save a snapshot of an aggregate reducing the amount of event needed to be fetched and applied.

```go
// saves the aggregate (an error will be returned if there are unsaved events on the aggregate when doing this operation)
SaveSnapshot(aggregate Aggregate) error
```

The repository constructor input values is an event store and a snapshot store, this handles the reading and writing of events and snapshots. We will dig deeper on the internals below.

```go
NewRepository(eventStore EventStore, snapshotStore SnapshotStore) *Repository
```

Here is an example of a person being saved and fetched from the repository.

```go
person := person.CreatePerson("Alice")
person.GrowOlder()
repo.Save(person)
twin := Person{}
repo.Get(person.Id, &twin)
```

### Event Store

The only thing an event store handles are events, and it must implement the following interface.

```go
// saves events to the under laying data store.
Save(events []eventsourcing.Event) error

// fetches events based on identifier and type but also after a specific version. The version is used to load event that happened after a snapshot was taken.
Get(id string, aggregateType string, afterVersion eventsourcing.Version) ([]eventsourcing.Event, error)

// return count number of events in global order starting at the start position
GlobalEvents(start, count uint64) ([]Event, error) {
```

Currently, there are three implementations.

* SQL
* Bolt
* RAM Memory

Post release v0.0.7 event stores `bbolt` and `sql` is their own submodules.
This reduces the dependency graph of the `github.com/hallgren/eventsourcing` module, as each submodule contains their own dependencies not pollute the main module.
Submodules needs to be fetched separately via go get.

`go get github.com/hallgren/eventsourcing/eventstore/sql`  
`go get github.com/hallgren/eventsourcing/eventstore/bbolt`

The memory based event store is part of the main module and does not need to be fetched separately.

### Snapshot Handler and Snapshot Store

A snapshot store save and get aggregate snapshots. A snapshot is a fix state of an aggregate on a specific version. The properties of an aggregate have to be exported for them to be saved in the snapshot.

The Snapshot Handler is the top layer that integrates with the repository.

```go
// Save transform an aggregate to a snapshot
Save(a Aggregate) error {

// Get fetch a snapshot and reconstruct an aggregate
Get(id string, a Aggregate) error {
```

A Snapshot store is the actual layer that stores the snapshot.

```go
// get snapshot by identifier
Get(id, typ string) (eventsource.Snapshot, error)

// saves snapshot
Save(s eventsourcing.Snapshot) error
```

Currently, there are two implementations of the snapshot store.

* SQL
* RAM Memory

Where the SQL snapshot store is a submodule and can be fetched via `go get github.com/hallgren/eventsourcing/snapshotstore/sql`

## Serializer

To store events and snapshots they have to be serialised into `[]byte`. This is handled differently depending on event
store implementation. The sql event store only marshal the event.Data and event.MetaData properties. (The rest is stored
in separate columns), while the bbolt event store marshal the hole event in its key / value database. The memory based event
store does not use a serializer due to it never serialise events to `[]byte`.

To be open to different storage solution the serializer takes as parameter to its constructor a marshal and unmarshal function,
that follows the declaration from the `"encoding/json"` package.

```go
NewSerializer(marshalF func (v interface{}) ([]byte, error), unmarshalF func(data []byte, v interface{}) error) *Serializer

creating a json based serializer: 
serializer := NewSerializer(json.Marshal, json.Unmarshal)
```

The registered event function is used internally inside the event store to set the correct type info when unmarshalling
event data into the `eventsourcing.Event`.
```go
RegisterTypes(aggregate Aggregate, events ...func() interface{})

register the aggregate Person and the event Born:
serializer.RegisterTypes(&Person{}, func() interface{} { return &Born{}})
```

### Event Subscription

The repository expose four possibilities to subscribe to events in realtime as they are saved to the repository.

`SubscriberAll(func (e Event)) (*Subscription, error)` all event.

`SubscriberAggregateType(func (e Event), aggregates ...Aggregate) (*Subscription, error)` events bound to specific aggregate types. 
 
`SubscriberSpecificEvent(func (e Event), events ...interface{}) (*Subscription, error)` specific events. There are no restrictions that the events need
to come from the same aggregate, you can mix and match as you please.

`SubscriberSpecificAggregate(func (e Event), events ...Aggregate) (*Subscription, error)` events bound to specific aggregate based on type and identity. This makes it possible to get events pinpointed to one specific aggregate instance. 

The subscription is realtime and events that are saved before the call to one of the subscribers will not be exposed via the `func(e Event)` function. If the application 
depends on this functionality make sure to call Subscribe() function on the subscriber before storing events in the repository. 

The event subscription enables the application to make use of the reactive patterns and to make it more decoupled. Check out the [Reactive Manifesto](https://www.reactivemanifesto.org/) 
for more detailed information. 

If the application is in no need of realtime subscription the repository could be created with the `eventsourcing.NewRepositoryNoEventStream(eventStore EventStore, snapshot *SnapshotHandler) *Repository` function. All subscriber methods on the created repository will return error `eventsourcing.ErrNoEventStream`.

Example on how to set up the event subscription and consume the event `FrequentFlierAccountCreated`

```go
// Setup a memory based repository
repo := eventsourcing.NewRepository(memory.Create(), nil)

// subscriber that will trigger on every saved events
s, _ := repo.SubscriberAll(func(e eventsourcing.Event) {
    switch e := event.Data.(type) {
        case *FrequentFlierAccountCreated:
            // e now have type info
            fmt.Println(e)
        }
    }
)

// start subscription
s.Subscribe()

// stop subscription
s.Unsubscribe() 
```

## Custom made components

Parts of this package may not fulfill your application need, either it can be that the event or snapshot stores uses the wrong database for storage.

#### Event Store

A custom-made event store has to implement the following functions to fulfill the interface in the repository.  

```go
type EventStore interface {
    Save(events []Event) error
    Get(id string, aggregateType string, afterVersion Version) ([]Event, error)
    GlobalEvents(start, count uint64) ([]Event, error) {
}
```

#### Snapshot Store

If the snapshot store is the thing you need to change here is the interface you need to uphold.

```go
type SnapshotStore interface {
    Get(id string, a interface{}) error
    Save(id string, a interface{}) error
}
```
