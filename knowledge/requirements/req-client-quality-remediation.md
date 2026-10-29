---
kind: requirement
id: REQ-CLIENT-QUALITY-REMEDIATION
title: "Repair the native client quality review findings with verified releases"
status: Accepted
liveness: auto
tags: [desktop, quality, security, performance]
---

## Source

The operator requested a multidimensional client quality review on 2026-10-29,
then authorized fixing as many identified problems as necessary, opening PRs in
reviewable batches and merging only after code review and passing CI.

## Requirements

[REQ-CLIENT-QUALITY-REMEDIATION] Track all sixteen findings from the original
review through implementation, explicit verification and remaining qualification.
Prioritize article image network isolation, dependency security, bounded normal
attachments and same-source release gates. Continue with credential and temporary
file protection, fair history indexing, bounded profile data, Board draft
retention, approval discovery, native message links, media actions and accurate
encrypted capability documentation. Fix protocol metadata and reduce hotspots
when the associated behavior is changed.

Preserve working public image loading, account isolation, encrypted attachment
integrity, original unknown outcomes, idempotency keys and explicit approval.
Do not count a placeholder, skipped test, old artifact or lockfile-only advisory
match as current qualification. Each batch must identify its actual subset of
repairs; a merged batch does not close the remaining review.

## Verification

Use production-path regressions, appropriate native checks, self-review and
current-head CI. Confirm each individual platform job succeeds before merging;
a workflow's aggregate green status must not conceal continue-on-error failures.
Do not start paid inference just to replace unrelated missing evidence.

## SDK synchronization provenance

This file is an unchanged accepted-requirement body copied from
`hagency-client/knowledge/requirements/req-client-quality-remediation.md` on
2026-10-30, with this provenance section appended. It binds the narrow SDK
read-only identity prerequisite of R05; it does not claim the SDK repairs the
remaining Client findings. The independent SDK branch starts at
`a34339e2926c703736e8d66e01cdf9aa933ff4d2`.
