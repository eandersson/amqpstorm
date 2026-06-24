Changelog
=========

Version 3.1.3
-------------
- ``Channel.build_inbound_messages`` gained an ``empty_timeout``
  parameter that controls how long the ``break_on_empty`` loop waits
  for the inbound queue to stay continuously empty before exiting
  while a consumer is active (default ``1.0`` seconds, matching the
  previous behaviour). A falsy value (``None`` or ``0``) exits as soon
  as the queue is empty.
- ``Channel.process_data_events`` and ``Channel.start_consuming`` now
  return as soon as the inbound queue is drained instead of waiting
  out the ``break_on_empty`` grace period, restoring the consume
  responsiveness from 3.0.x that the 3.1.0 grace period had slowed
  (follow-up to #154).

Version 3.1.2
-------------
- The AMQP protocol header is now sent before the inbound reader
  thread is started, so the initial socket write no longer races the
  reader on the same TLS socket. This fixes intermittent
  ``connection closed by server`` errors during the handshake against
  brokers that send data immediately after the TLS handshake (for
  example TLS 1.3 session tickets). ``IO.open`` now only connects and
  prepares the poller; the reader is started separately via the new
  ``IO.start_inbound``.
- ``Rpc.on_frame`` no longer tears down the connection when the
  matching request or response has already been removed (for example
  when an RPC call times out); the stray frame is ignored instead.
- Fixed a large-message consume throughput regression in
  ``Channel.build_inbound_messages`` (#154).
- Added LavinMQ compatibility for the management API client; the
  functional and management test suites now run against LavinMQ.
- Testing: added an end-to-end LavinMQ CI job and modernized the test
  certificate generation to ECDSA P-384 with standards-compliant
  end-entity certificates.

Version 3.1.1
-------------
- Connection close is now resilient to ``IO.socket`` being cleared
  partway through ``_close_socket``; the socket reference is captured
  once and reused for the shutdown/close calls.

Version 3.1
-----------
- Added inline type hints across the public API and shipped ``py.typed``
  so downstream type checkers pick them up (PEP 561).
- ``Channel.build_inbound_messages`` now ends cleanly when the channel
  or connection is closed via user code (``.close()``); server- and
  network-driven closes still raise as before. ``check_for_errors``
  itself is unchanged.
- Performance: byte-string accumulation in receive / publish paths
  replaced with ``b''.join`` (no more O(n²) under non-CPython refcount
  semantics); writes use ``memoryview`` slicing; RPC response storage
  switched to ``collections.deque`` so popping multi-frame ``Basic.Get``
  results is O(n) instead of O(n²).
- Modernized to Python 3.11+ syntax: dropped ``(object)`` bases, switched
  to argument-less ``super()``, f-strings, modern ``with`` blocks for
  locks.
- Packaging migrated from ``setup.py`` / ``setup.cfg`` to PEP 621
  ``pyproject.toml``; version is now read dynamically from
  ``amqpstorm.__version__``.
- Documentation: fixed several typos, completed missing ``:members:``
  entries on the management API autodoc, added intersphinx mapping and
  short type-hint rendering in the Sphinx config.
- SSL: hostname verification now on by default; unknown ``cert_reqs``
  URI values fall back to ``CERT_REQUIRED`` instead of ``CERT_NONE``.
- Switched all elapsed-duration checks (RPC and connection-state
  timeouts) from ``time.time()`` to ``time.monotonic()`` so system
  clock changes can no longer cause spurious timeouts or hangs.

Version 3.0
-----------
- Python 3 only release.
- Added support for pamqp 4.x.
- Improved SSL handling.