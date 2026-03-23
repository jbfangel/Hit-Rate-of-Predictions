# Bug Fix List

## Database Fixes

- [x] **'Chrono des nations' wrongly classified as RR** — should be `TT` (time trial), not `RR` (road race). Fix: update `prediction_type` (or equivalent race type column) for this race in `data/predictions.db`.
