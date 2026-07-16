"""Simulated external service: a separate local HTTP process.

See `docs/specs/features/simulated-external-service.md` for the approved
contract. This package is reached only through its HTTP surface; it must
never be imported by `boolean_maybe.cli` or application code.
"""
