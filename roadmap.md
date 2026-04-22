# Development Roadmap

## v0.4.0

- Calendar factory should return an instance of AbstractCalendar class
  - AbstractCalendar should declare the business day interface
- [X] Add `core` submodule
  - [X] Refactor `Lookup` into `lookup.py`
    - Adds namespace validation
  - [X] Refactor dependency mixins into `dependency.py`
  - [X] Refactor exceptions
- [X] Add `event` subgraph
  - [X] `event.sender`
  - `event.archiver`
  - `event.replayer`
- [X] 100% test coverage
- Make pandas + market calendar dependencies optional
- Rename project to `modular-graph`
- [X] Add mypy 1.20 to pre-commit config
