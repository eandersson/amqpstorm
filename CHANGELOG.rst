Changelog
=========

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

Version 3.0
-----------
- Python 3 only release.
- Added support for pamqp 4.x.
- Improved SSL handling.