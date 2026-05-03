import 'dart:async';

import 'package:debounced_grouped_stream/debounced_grouped_stream.dart';
import 'package:test/test.dart';

void main() {
  test('groups events by key and debounces them', () async {
    final controller = StreamController<String>();
    final dgs = DebouncedGroupedStream(controller.stream);

    final batches = <List<String>>[];
    final sub = dgs.listen(
      groupBy: [(e) => e.split(':').first], // group by prefix before ':'
      debounce: const Duration(milliseconds: 50),
    ).listen(batches.add);

    // Three events for group 'a', two for group 'b' — all within debounce window.
    controller.add('a:1');
    controller.add('a:2');
    controller.add('b:1');
    controller.add('a:3');
    controller.add('b:2');

    // Wait for debounce timers to fire.
    await Future<void>.delayed(const Duration(milliseconds: 100));

    expect(batches, containsAll([
      ['a:1', 'a:2', 'a:3'],
      ['b:1', 'b:2'],
    ]));

    await sub.cancel();
    dgs.dispose();
  });

  test('two subscribers have independent grouping and debounce state', () async {
    final controller = StreamController<String>();
    final dgs = DebouncedGroupedStream(controller.stream);

    final batchesA = <List<String>>[];
    final batchesB = <List<String>>[];

    // Subscriber A: groups by prefix, 50 ms debounce.
    final subA = dgs.listen(
      groupBy: [(e) => e.split(':').first],
      debounce: const Duration(milliseconds: 50),
    ).listen(batchesA.add);

    // Subscriber B: groups by suffix (after ':'), 80 ms debounce.
    final subB = dgs.listen(
      groupBy: [(e) => e.split(':').last],
      debounce: const Duration(milliseconds: 80),
    ).listen(batchesB.add);

    controller.add('a:1');
    controller.add('b:1');
    controller.add('a:2');
    controller.add('c:1');

    // After 50 ms: A's timers fire; B's have not yet.
    await Future<void>.delayed(const Duration(milliseconds: 60));
    expect(batchesA, containsAll([['a:1', 'a:2'], ['b:1'], ['c:1']]));
    expect(batchesB, isEmpty); // B still debouncing

    // After 80 ms total: B's timers fire.
    await Future<void>.delayed(const Duration(milliseconds: 40));
    // B groups by suffix: '1' → [a:1, b:1, c:1], '2' → [a:2].
    expect(batchesB, containsAll([['a:1', 'b:1', 'c:1'], ['a:2']]));

    await subA.cancel();
    await subB.cancel();
    dgs.dispose();
  });

  test('cancelling one subscriber does not affect the other', () async {
    final controller = StreamController<String>();
    final dgs = DebouncedGroupedStream(controller.stream);

    final batchesA = <List<String>>[];
    final batchesB = <List<String>>[];

    final subA = dgs.listen(
      groupBy: [(e) => e.split(':').first],
      debounce: const Duration(milliseconds: 50),
    ).listen(batchesA.add);

    final subB = dgs.listen(
      groupBy: [(e) => e.split(':').first],
      debounce: const Duration(milliseconds: 50),
    ).listen(batchesB.add);

    controller.add('a:1');
    controller.add('a:2');

    // Cancel A before debounce fires.
    await subA.cancel();

    await Future<void>.delayed(const Duration(milliseconds: 100));

    expect(batchesA, isEmpty);
    expect(batchesB, [['a:1', 'a:2']]);

    await subB.cancel();
    dgs.dispose();
  });
}
