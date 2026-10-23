import 'dart:async';
import 'dart:collection';

/// Bounded-concurrency upload runner.
///
/// Sending used to be a strict one-at-a-time affair: the composer locked
/// until each file finished uploading, so five photos meant five sequential
/// waits. Uploads now run in parallel up to [maxConcurrent] at a time —
/// enough to feel instant like Telegram, few enough not to overwhelm a
/// shared-hosting backend (or the phone's own radio) with parallel sockets.
/// Extra work queues FIFO and starts as slots free up.
class UploadQueue {
  UploadQueue({this.maxConcurrent = 3});

  static final UploadQueue instance = UploadQueue();

  final int maxConcurrent;

  int _running = 0;
  final Queue<Completer<void>> _waiters = Queue<Completer<void>>();

  int get running => _running;
  int get waiting => _waiters.length;

  Future<T> run<T>(Future<T> Function() task) async {
    if (_running >= maxConcurrent) {
      final completer = Completer<void>();
      _waiters.add(completer);
      await completer.future;
      // The finishing task transferred its slot to us; _running already
      // accounts for this task, so it must not be incremented again.
    } else {
      _running++;
    }
    try {
      return await task();
    } finally {
      if (_waiters.isNotEmpty) {
        // Hand the slot straight to the next waiter instead of freeing it,
        // so a newcomer can never overtake the queue and exceed the cap.
        _waiters.removeFirst().complete();
      } else {
        _running--;
      }
    }
  }
}
