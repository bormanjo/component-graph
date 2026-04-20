# Development Roadmap

## v0.4.0

- Calendar factory should return an instance of AbstractCalendar class
  - AbstractCalendar should declare the business day interface
- Add `_core` submodule with `core.py` underneath
  - [X] Refactor `Lookup` into `lookup.py`
    - Adds namespace validation
  - [ ] Refactor dependency mixins into `mixins.py`
  - [ ] Refactor exceptions
- Add `event` subgraph
  - `event.sender`
  - `event.archiver`
  - `event.replayer`
