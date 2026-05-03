import 'dart:async';

import 'package:debounced_grouped_stream/debounced_grouped_stream.dart';

void main() async {
  final controller = StreamController<String>();
  final dgs = DebouncedGroupedStream(controller.stream);

  // Subscriber A: groups by the prefix before ':', 100 ms debounce.
  dgs
      .listen(
        groupBy: [(e) => e.split(':').first],
        debounce: const Duration(milliseconds: 100),
      )
      .listen((batch) => print('[A] ${batch.first.split(':').first}: $batch'));

  // Subscriber B: groups by the suffix after ':', 150 ms debounce.
  dgs
      .listen(
        groupBy: [(e) => e.split(':').last],
        debounce: const Duration(milliseconds: 150),
      )
      .listen((batch) => print('[B] suffix=${batch.first.split(':').last}: $batch'));

  // Emit a burst of events.
  for (final event in ['user:1', 'order:1', 'user:2', 'order:2', 'user:3']) {
    controller.add(event);
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }

  // Wait for all debounce timers to settle.
  await Future<void>.delayed(const Duration(milliseconds: 300));

  dgs.dispose();
  await controller.close();
}
