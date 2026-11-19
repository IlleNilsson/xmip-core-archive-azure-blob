# xmip-core-archive-azure-blob

Azure Blob Storage archive target: one item is one blob in a container, its TOML metadata a second blob under `.meta`, each created only where none is and never written over. A technology of [xmip-core-archive](https://github.com/IlleNilsson/xmip-core-archive).

## How an item is kept

An archived item is never written over (ADR-0040, amendment 2026-11-19). The rule is the archive capability's, `archive::named`, once for every store laid out by name; this crate is the store under it, its `Named`. The metadata, a TOML sidecar holding the data type, the identifier, when, the SHA-256 of the bytes and the pairs, is created first at `<prefix>/.meta/<data_type>/<identifier>`: it is the claim. The bytes are created next at `<prefix>/<data_type>/<identifier>`, and only then is there a receipt. Each create is Put Blob with `If-None-Match: *` through the transport's `Client::put_new`, so a blob already there is left as it was: the same item archived again answers the same receipt, another item or other bytes at the name are refused, and an archive stopped between the two blobs is completed by its repeat. A restore reads the metadata first and refuses a blob without it; the receipt's checksum, the metadata's and the bytes' must agree. A data type beginning with a dot is refused, so `order` and `order.meta` are two items.

The receipt is `azure-blob://<container>/<blob>`, the transport's origin for the blob (`azure_blob::origin`); a receipt naming another container is not this archive's. Until 2026-11-19 an archive put the bytes and then the metadata, each over whatever lay there, at `<blob>` and `<blob>.meta`, and the receipt named the account as well.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
