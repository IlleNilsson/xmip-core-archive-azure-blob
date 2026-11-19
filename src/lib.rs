#![forbid(unsafe_code)]

//! Azure Blob archive: an [`ArchiveStore`] that keeps each retained item as
//! one block blob in a container, its metadata as a second blob, and
//! restores the item by getting both back.
//!
//! What an archive laid out by name does is the archive capability's, once
//! for every such store (`archive::named`, ADR-0040, amendment 2026-11-19):
//! the metadata, a TOML sidecar with the checksum, is created first at
//! `<prefix>/.meta/<data_type>/<identifier>` as the claim, the bytes next at
//! `<prefix>/<data_type>/<identifier>`, and only then is there a receipt;
//! an archived item is never written over, and a restore reads the
//! metadata first. This crate is the [`Named`] store under it: a create is
//! Put Blob with `If-None-Match: *` (`azure_blob::Client::put_new`), which
//! leaves a blob already there as it was and says so; a read is Get Blob,
//! none where the service answers 404 `BlobNotFound`.
//!
//! A xmip-core-archive **technology** (repository-model.md): it depends on
//! the archive capability for the [`ArchiveStore`] and [`Named`] traits and
//! the item, receipt and error types, and on the Azure Blob transport
//! technology for the signed requests — Shared Key over the Blob REST API.
//!
//! An archive never deletes (ADR-0040): this one creates and gets, nothing
//! else. The receipt is `azure-blob://<container>/<blob>`, the transport's
//! origin for the blob, written by its `origin` and read back against it,
//! with the SHA-256 of the bytes as its checksum.

use std::time::Duration;

use archive::named::{self, Created, Named};
use archive::{ArchiveError, ArchiveItem, ArchiveReceipt, ArchiveStore};
use azure_blob::Client;

/// An archive that keeps items as blobs under one prefix of one container.
pub struct AzureBlobArchive {
    endpoint: String,
    account: String,
    key_base64: String,
    container: String,
    prefix: String,
    timeout: Option<Duration>,
}

impl AzureBlobArchive {
    /// An archive writing to `container` at `endpoint` —
    /// `https://account.blob.core.windows.net` in the cloud,
    /// `http://127.0.0.1:10000` for Azurite — as `account` with its key,
    /// base64, under no prefix.
    #[must_use]
    pub fn new(
        endpoint: impl Into<String>,
        account: impl Into<String>,
        key_base64: impl Into<String>,
        container: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            account: account.into(),
            key_base64: key_base64.into(),
            container: container.into(),
            prefix: String::new(),
            timeout: None,
        }
    }

    /// The prefix every blob name is laid out under, `retained` say.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Give up on an endpoint that stops answering.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn client(&self) -> Result<Client, ArchiveError> {
        let client = Client::new(&self.endpoint, &self.account, &self.key_base64)
            .map_err(ArchiveError::caused_by)?;
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }
}

impl ArchiveStore for AzureBlobArchive {
    fn archive(&self, item: ArchiveItem) -> Result<ArchiveReceipt, ArchiveError> {
        named::archive(self, &self.prefix, item)
    }

    fn restore(&self, receipt: &ArchiveReceipt) -> Result<ArchiveItem, ArchiveError> {
        named::restore(self, &self.prefix, receipt)
    }
}

impl Named for AzureBlobArchive {
    fn create(&self, key: &str, bytes: &[u8]) -> Result<Created, ArchiveError> {
        let stored = self
            .client()?
            .put_new(&self.container, key, bytes)
            .map_err(ArchiveError::caused_by)?;
        Ok(if stored {
            Created::Created
        } else {
            Created::Exists
        })
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ArchiveError> {
        self.client()?
            .get(&self.container, key)
            .map_err(ArchiveError::caused_by)
    }

    fn location(&self, key: &str) -> String {
        azure_blob::origin(&self.container, key)
    }

    /// The blob a receipt names in this archive's container: what follows
    /// the origin of the container itself.
    fn key(&self, location: &str) -> Result<String, ArchiveError> {
        location
            .strip_prefix(&self.location(""))
            .filter(|blob| !blob.is_empty())
            .map(str::to_string)
            .ok_or_else(|| ArchiveError {
                message: format!(
                    "{location} is not a blob in this archive's container, {}",
                    self.location("<blob>")
                ),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use archive::checksum::sha256_hex;
    use archive::fixture::{item, secs};
    use azure_blob::{Event, Session};
    use std::assert_matches;
    use std::net::{TcpListener, TcpStream};
    use std::thread::JoinHandle;

    /// `secret`, base64.
    const KEY: &str = "c2VjcmV0";
    const CONTAINER: &str = "orders-archive";

    /// A far end answering every signed request, one connection each,
    /// until the test is [`FarEnd::done`] with it.
    struct FarEnd {
        address: String,
        serving: JoinHandle<(Session, Vec<Event>)>,
    }

    impl FarEnd {
        fn new() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            let address = listener.local_addr().expect("address").to_string();
            let serving = std::thread::spawn(move || {
                let mut session = Session::new("acct", KEY)
                    .expect("base64")
                    .timing_out_after(secs(2));
                let mut events = Vec::new();
                // A connection that sends nothing is the test saying done.
                while let Ok(event) = session.serve_one(&listener) {
                    events.push(event);
                }
                (session, events)
            });
            Self { address, serving }
        }

        fn store(&self) -> AzureBlobArchive {
            AzureBlobArchive::new(format!("http://{}", self.address), "acct", KEY, CONTAINER)
                .with_prefix("retained")
                .timing_out_after(secs(2))
        }

        /// A client beside the archive, for what the archive did not do.
        fn client(&self) -> Client {
            Client::new(&format!("http://{}", self.address), "acct", KEY)
                .expect("endpoint")
                .timing_out_after(secs(2))
        }

        /// What the far end holds and what it saw.
        fn done(self) -> (Session, Vec<Event>) {
            drop(TcpStream::connect(&self.address));
            self.serving.join().expect("thread")
        }
    }

    fn stored(events: &[Event]) -> usize {
        events
            .iter()
            .filter(|event| matches!(event, Event::Stored(_)))
            .count()
    }

    fn blob(key: &str) -> String {
        format!("azure-blob://{CONTAINER}/{key}")
    }

    #[test]
    fn an_archived_item_is_its_metadata_then_its_bytes_and_the_receipt_names_the_bytes() {
        let far_end = FarEnd::new();
        let original = item("json#1");
        let receipt = far_end.store().archive(original.clone()).expect("archive");
        assert_eq!(receipt.location, blob("retained/json/json#1"));
        assert_eq!(
            receipt.location,
            azure_blob::origin(CONTAINER, "retained/json/json#1"),
            "the transport's origin"
        );
        assert_eq!(
            receipt.checksum.as_deref(),
            Some(sha256_hex(&original.bytes).as_str())
        );
        let (session, events) = far_end.done();
        let held = session.blobs();
        assert_eq!(held.len(), 2);
        assert_eq!(
            held.get("orders-archive/retained/json/json#1"),
            Some(&original.bytes)
        );
        let sidecar = String::from_utf8(held["orders-archive/retained/.meta/json/json#1"].clone())
            .expect("text");
        assert!(sidecar.starts_with("[item]\n"), "{sidecar}");
        assert!(sidecar.contains(&sha256_hex(&original.bytes)), "{sidecar}");
        assert_matches!(
            &events[0],
            Event::Stored(claim) if claim.origin_uri == blob("retained/.meta/json/json#1")
        );
        assert_matches!(
            &events[1],
            Event::Stored(bytes) if bytes.origin_uri == blob("retained/json/json#1")
        );
    }

    #[test]
    fn the_blobs_restore_the_item_through_the_same_session() {
        let far_end = FarEnd::new();
        let store = far_end.store();
        let original = item("json#2");
        let receipt = store.archive(original.clone()).expect("archive");
        let restored = store.restore(&receipt).expect("restore");
        assert_eq!(restored, original, "both blobs read back give the item");
        let (_, events) = far_end.done();
        assert_eq!(
            events[2..],
            [
                Event::Retrieved(blob("retained/.meta/json/json#2")),
                Event::Retrieved(blob("retained/json/json#2"))
            ],
            "the metadata first"
        );
    }

    #[test]
    fn an_identifier_archived_again_with_the_same_bytes_answers_the_same_receipt() {
        let far_end = FarEnd::new();
        let store = far_end.store();
        let first = store.archive(item("json#3")).expect("archive");
        let again = store.archive(item("json#3")).expect("archived again");
        assert_eq!(again, first);
        let (session, events) = far_end.done();
        assert_eq!(stored(&events), 2, "nothing written over");
        assert_eq!(session.blobs().len(), 2);
    }

    #[test]
    fn an_identifier_archived_again_with_other_bytes_is_refused_and_the_earlier_receipt_restores() {
        let far_end = FarEnd::new();
        let store = far_end.store();
        let original = item("json#4");
        let receipt = store.archive(original.clone()).expect("archive");
        let mut other = original.clone();
        other.bytes = b"{\"kept\":false}".to_vec();
        let refused = store.archive(other).expect_err("other bytes");
        assert!(
            refused
                .message
                .contains("is archived already with other bytes"),
            "{refused}"
        );
        assert_eq!(store.restore(&receipt).expect("restore"), original);
        let (_, events) = far_end.done();
        assert_eq!(stored(&events), 2, "nothing written over");
    }

    #[test]
    fn order_and_order_meta_are_two_items() {
        let far_end = FarEnd::new();
        let store = far_end.store();
        let order = item("order");
        let mut order_meta = item("order.meta");
        order_meta.bytes = b"order.meta's own bytes".to_vec();
        order_meta.metadata = vec![("source".to_string(), "elsewhere".to_string())];
        let receipts = [
            store.archive(order.clone()).expect("order"),
            store.archive(order_meta.clone()).expect("order.meta"),
        ];
        assert_eq!(store.restore(&receipts[0]).expect("order"), order);
        assert_eq!(store.restore(&receipts[1]).expect("order.meta"), order_meta);
        let (session, _) = far_end.done();
        assert_eq!(session.blobs().len(), 4);
    }

    #[test]
    fn an_interrupted_pair_is_completed_by_its_repeat_and_refuses_another_item() {
        const BYTES: &str = "retained/json/json#5";
        let far_end = FarEnd::new();
        let store = far_end.store();
        let original = item("json#5");
        store.archive(original.clone()).expect("archive");
        // Stopped between the two: the claim made, the bytes not there.
        let client = far_end.client();
        let (_, etag) = client
            .list(CONTAINER, BYTES)
            .expect("list")
            .into_iter()
            .find(|(name, _)| name == BYTES)
            .expect("listed");
        client
            .delete_as_listed(CONTAINER, BYTES, &etag)
            .expect("deleted");
        let mut other = original.clone();
        other.bytes = b"another item".to_vec();
        let refused = store.archive(other).expect_err("another item");
        assert!(
            refused
                .message
                .contains("is archived already with other bytes"),
            "{refused}"
        );
        let receipt = store.archive(original.clone()).expect("completed");
        assert_eq!(store.restore(&receipt).expect("restore"), original);
        let (session, _) = far_end.done();
        assert_eq!(session.blobs().len(), 2);
    }

    #[test]
    fn bytes_without_metadata_are_refused_on_restore() {
        let far_end = FarEnd::new();
        let original = item("json#6");
        far_end
            .client()
            .put(CONTAINER, "retained/json/json#6", &original.bytes)
            .expect("put");
        let receipt = ArchiveReceipt {
            location: blob("retained/json/json#6"),
            checksum: Some(sha256_hex(&original.bytes)),
        };
        let refused = far_end.store().restore(&receipt).expect_err("no metadata");
        assert!(
            refused
                .message
                .contains("is not an item this archive completed"),
            "{refused}"
        );
        far_end.done();
    }

    #[test]
    fn a_foreign_object_at_the_key_is_refused() {
        let far_end = FarEnd::new();
        let store = far_end.store();
        let client = far_end.client();
        client
            .put(CONTAINER, "retained/json/json#7", b"foreign")
            .expect("put");
        let refused = store.archive(item("json#7")).expect_err("foreign bytes");
        assert!(
            refused
                .message
                .contains("holds bytes this archive did not write there"),
            "{refused}"
        );
        client
            .put(CONTAINER, "retained/.meta/json/json#8", b"foreign")
            .expect("put");
        let refused = store.archive(item("json#8")).expect_err("foreign metadata");
        assert!(
            refused.message.contains("is not an archive's metadata"),
            "{refused}"
        );
        let (session, _) = far_end.done();
        let held = session.blobs();
        assert_eq!(held["orders-archive/retained/json/json#7"], b"foreign");
        assert_eq!(
            held["orders-archive/retained/.meta/json/json#8"],
            b"foreign"
        );
        assert!(!held.contains_key("orders-archive/retained/json/json#8"));
    }

    #[test]
    fn a_data_type_beginning_with_a_dot_is_refused() {
        let far_end = FarEnd::new();
        let mut dotted = item("json#9");
        dotted.data_type = ".meta".to_string();
        let refused = far_end.store().archive(dotted).expect_err("a dot first");
        assert!(refused.message.contains("begins with a dot"), "{refused}");
        let (session, events) = far_end.done();
        assert!(events.is_empty(), "no request is made");
        assert!(session.blobs().is_empty());
    }

    #[test]
    fn a_changed_checksum_a_missing_item_and_a_foreign_receipt_are_refused() {
        let far_end = FarEnd::new();
        let store = far_end.store();
        let mut receipt = store.archive(item("json#10")).expect("archive");
        receipt.checksum = Some("0".repeat(64));
        let changed = store.restore(&receipt).expect_err("checksum");
        assert!(changed.message.contains("checksum mismatch"), "{changed}");
        receipt.location = blob("retained/json/none");
        receipt.checksum = None;
        let missing = store.restore(&receipt).expect_err("missing");
        assert!(
            missing
                .message
                .contains("is not an item this archive completed"),
            "{missing}"
        );
        far_end.done();
        for location in [
            "aws-s3://bucket/key",
            "azure-blob://orders-archive",
            "azure-blob://orders-archive/",
            "azure-blob://elsewhere/retained/json/n",
            "azure-blob://orders-archive/elsewhere/n",
        ] {
            receipt.location = location.to_string();
            assert!(store.restore(&receipt).is_err(), "{location}");
        }
        let wrong_key = AzureBlobArchive::new("http://127.0.0.1:1", "acct", "not base64!", "c");
        assert!(
            wrong_key.archive(item("json#11")).is_err(),
            "no request is made"
        );
    }
}
