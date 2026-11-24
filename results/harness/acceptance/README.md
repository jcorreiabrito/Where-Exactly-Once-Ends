# Acceptance evidence archive (S3 hand-off, archived 2026-11-24T14:54:46Z)

Source: %LOCALAPPDATA%\eos-runtime\run\acceptance\ (runtime, not committed).
The committed copies are path-sanitized; the original SHA-256 values are recorded here.

- `acceptance-result.json`: original_sha256=53C58600B5A8CFF01B06F9DFEBEEE16F8C3C9564D95DD98D077CE39974414A35 original_bytes=38045 original_mtime_utc=2026-11-24T14:37:30Z archived_sha256=B45EB7EB26EEAAEFC6BF9B26D1520D89E9CEA78290307692EC270EB19B637A54
- `acceptance-suite.log`: original_sha256=56F5E17A7F30C574A0A55F6523E19DD4ECCA1906A695678265C5F3F5F4AF364C original_bytes=1894 original_mtime_utc=2026-11-24T14:37:30Z archived_sha256=7D16F75F6D1102314CE578CA6FB9A9DB4EEED80EBEAE271C44DEEA09FEE03EF4
- `wxeo-service.jar`: sha256=6E8223751706D64E7B77752B42DF5694D4231BA8BD3522034806BAE25E8E50D3 bytes=34467687

This evidence covers: Kafka create/produce/consume; M0 E2E 1000 events -> 2000/2000 postings, 0 duplicates; M3 E2E identical; crash failpoint exit=99 with exactly one fired receipt and no restored receipt; stack stopped.
