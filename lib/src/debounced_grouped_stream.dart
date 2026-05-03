import 'dart:async';

/// Wraps a [Stream<T>] and lets each subscriber independently debounce and
/// group incoming events before delivery.
///
/// Usage:
/// ```dart
/// final dgs = DebouncedGroupedStream(rawStream);
///
/// final sub = dgs.listen(
///   groupBy: [(event) => event.userId],
///   debounce: Duration(milliseconds: 300),
/// ).listen((batch) => print(batch));
///
/// dgs.dispose();
/// ```
class DebouncedGroupedStream<T> {
  DebouncedGroupedStream(Stream<T> source) {
    _sourceSub = source.listen(_onEvent, onError: _controller.addError, onDone: _controller.close);
  }

  final _controller = StreamController<T>.broadcast();
  late final StreamSubscription<T> _sourceSub;

  void _onEvent(T event) => _controller.add(event);

  /// Returns a stream that delivers events as debounced batches.
  ///
  /// [groupBy] — one or more key extractors. Keys must be unique across
  /// extractors. Events matching the same key are accumulated and delivered
  /// together after [debounce] of silence. Unmatched events are forwarded
  /// immediately as a single-element list.
  ///
  /// Throws [ArgumentError] if [groupBy] is empty.
  Stream<List<T>> listen({
    required List<Object? Function(T)> groupBy,
    required Duration debounce,
  }) {
    if (groupBy.isEmpty) throw ArgumentError.value(groupBy, 'groupBy', 'must not be empty');

    final outController = StreamController<List<T>>();
    // key → accumulated events
    final Map<Object?, List<T>> buckets = {};
    // key → active timer
    final Map<Object?, Timer> timers = {};

    void flush(Object? key) {
      timers.remove(key)?.cancel();
      final batch = buckets.remove(key);
      if (batch != null && batch.isNotEmpty) outController.add(batch);
    }

    final sub = _controller.stream.listen(
      (T event) {
        Object? matchedKey;
        for (final extractor in groupBy) {
          final key = extractor(event);
          if (key != null) {
            matchedKey = key;
            break;
          }
        }

        if (matchedKey == null) {
          // Unmatched — forward immediately.
          outController.add([event]);
          return;
        }

        buckets.putIfAbsent(matchedKey, () => []).add(event);
        timers[matchedKey]?.cancel();
        timers[matchedKey] = Timer(debounce, () => flush(matchedKey));
      },
      onError: outController.addError,
      onDone: () {
        // Flush all pending buckets before closing.
        for (final key in buckets.keys.toList()) { flush(key); }
        outController.close();
      },
    );

    outController.onCancel = () {
      sub.cancel();
      for (final t in timers.values) { t.cancel(); }
      timers.clear();
      buckets.clear();
    };

    return outController.stream;
  }

  void dispose() {
    _sourceSub.cancel();
    _controller.close();
  }
}

