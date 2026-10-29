//! Single-use owner-device room ciphertext; independent of the room's reusable
//! outbound group session and cached member list.
use super::Room;
use crate::{
    Error, Result, config::RequestConfig, room::futures::SendMessageLikeEventResult,
    utils::IntoRawMessageLikeEventContent,
};
use matrix_sdk_base::{
    crypto::types::{
        events::room::encrypted::RoomEncryptedEventContent, requests::ToDeviceRequest,
    },
    deserialized_responses::EncryptionInfo,
};
use ruma::serde::Raw;
use ruma::{
    TransactionId,
    api::client::{message::send_message_event, to_device::send_event_to_device},
};

/// One sealed event and encrypted key-share batch for cross-signed own devices.
/// Not Clone or serializable. Sending consumes it; callers must durably reserve
/// the original publication and retain this operation through UI cancellation.
#[allow(missing_debug_implementations)]
pub struct OwnDeviceMessage {
    room: Room,
    content: Raw<RoomEncryptedEventContent>,
    encryption_info: EncryptionInfo,
    shares: Vec<ToDeviceRequest>,
}
impl Room {
    /// Seal an event for this account's cross-signed remote devices only, using
    /// a fresh Megolm key which is never put in the reusable outbound room store.
    /// This claims device keys but sends no event or session key. Callers must
    /// separately validate private-room state and current application authority.
    pub async fn seal_for_own_devices(
        &self,
        event_type: &str,
        content: impl IntoRawMessageLikeEventContent,
    ) -> Result<OwnDeviceMessage> {
        self.ensure_room_joined()?;
        if !self.latest_encryption_state().await?.is_encrypted() {
            return Err(Error::EncryptionRequired);
        }
        let owner = self.own_user_id();
        let identity = self
            .client
            .encryption()
            .request_user_identity(owner)
            .await?
            .ok_or(Error::InsufficientData)?;
        let device =
            self.client.encryption().get_own_device().await?.ok_or(Error::InsufficientData)?;
        if !identity.is_verified()
            || !device.is_verified_with_cross_signing()
            || device.is_deleted()
        {
            return Err(Error::InsufficientData);
        }
        let _guard = self.client.encryption().spin_lock_store(Some(60000)).await?;
        self.client.claim_one_time_keys(std::iter::once(owner)).await?;
        let machine = self.client.olm_machine().await;
        let machine = machine.as_ref().ok_or(Error::NoOlmMachine)?;
        let (encrypted, shares) = machine
            .seal_room_event_for_own_devices(
                self.room_id(),
                event_type,
                &content.into_raw_message_like_event_content(),
            )
            .await?;
        Ok(OwnDeviceMessage {
            room: self.clone(),
            content: encrypted.content,
            encryption_info: encrypted.encryption_info,
            shares,
        })
    }
}
impl OwnDeviceMessage {
    /// Consume the original sealed event. Check current application authority
    /// before each write and after each key-share response. SDK retries are
    /// disabled; failure or cancellation never returns another send capsule.
    pub async fn send(
        self,
        transaction_id: &TransactionId,
        live: impl Fn() -> bool,
    ) -> Result<SendMessageLikeEventResult> {
        let config = Some(RequestConfig::new().disable_retry());
        for share in self.shares {
            if !live() {
                return Err(Error::InsufficientData);
            }
            let request = send_event_to_device::v3::Request::new_raw(
                share.event_type,
                share.txn_id,
                share.messages,
            );
            self.room.client.send(request).with_request_config(config).await?;
        }
        if !live() {
            return Err(Error::InsufficientData);
        }
        self.room.ensure_room_joined()?;
        let request = send_message_event::v3::Request::new_raw(
            self.room.room_id().to_owned(),
            transaction_id.to_owned(),
            "m.room.encrypted".into(),
            self.content.cast(),
        );
        let response = self.room.client.send(request).with_request_config(config).await?;
        // Keep the actual acceptance even if application authority retires just
        // afterwards; the owner journal records a late original receipt only.
        Ok(SendMessageLikeEventResult { response, encryption_info: Some(self.encryption_info) })
    }
}
