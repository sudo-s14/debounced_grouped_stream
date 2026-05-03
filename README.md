# debounced_grouped_stream

A Dart stream wrapper that lets each subscriber independently debounce and group incoming events by key before delivery.

## Features

- **Per-subscriber configuration** — each call to `listen()` gets its own grouping keys and debounce duration, so multiple subscribers on the same source behave independently.
- **Key-based grouping** — supply one or more key extractor functions; events that produce the same key are accumulated into a single batch.
- **Debounce per group** — the timer resets on every new event in a group and fires only after the specified period of silence.
- **Unmatched event passthrough** — events that don't match any key extractor are forwarded immediately as a single-element list.
- **Clean teardown** — cancelling a subscriber flushes pending timers; disposing the root stream closes all subscriptions.

## Usage

```dart
import 'package:debounced_grouped_stream/debounced_grouped_stream.dart';

final dgs = DebouncedGroupedStream(rawStream);

// Group by userId, batch events that arrive within 300 ms of each other.
final subscription = dgs.listen(
  groupBy: [(event) => event.userId],
  debounce: Duration(milliseconds: 300),
).listen((batch) {
  print('Received ${batch.length} events for user ${batch.first.userId}');
});

// Multiple independent subscribers on the same source:
final otherSub = dgs.listen(
  groupBy: [(event) => event.category],
  debounce: Duration(seconds: 1),
).listen((batch) => processByCategory(batch));

// Clean up
subscription.cancel();
otherSub.cancel();
dgs.dispose();
```

## Getting started

Add the dependency to your `pubspec.yaml`:

```yaml
dependencies:
  debounced_grouped_stream: ^1.0.0
```

No additional setup is required — the package has no platform dependencies.

## Additional information

- File issues and feature requests on the project repository.
- Pull requests are welcome.
- Licensed under the [MIT License](LICENSE).
