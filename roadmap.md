# Development Roadmap

## v0.4.0

- [X] Add `core` submodule
  - [X] Refactor `Lookup` into `lookup.py`
    - [X] Adds namespace validation
  - [X] Refactor dependency mixins into `dependency.py`
  - [X] Refactor exceptions
- [X] Add `event` subgraph
  - [X] `event.sender`
  - [X] `event.archiver`
  - [X] `event.replayer`
- [X] 100% test coverage
- [X] Make pandas + market calendar dependencies optional
- Rename project to `modular-graph`
- [X] Add mypy 1.20 to pre-commit config

## v0.5.0

- Calendar factory should return an instance of AbstractCalendar class
  - AbstractCalendar should declare the business day interface
