//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

extern crate serde;

use crate::{ZFData, ZFError, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uhlc::Timestamp;
use uuid::Uuid;

use std::time::Duration;

// ser/de
use zenoh::net::protocol::io::*;

pub type NodeId = u64;
pub type PortId = u64;
pub type RuntimeId = u64;
pub type FlowId = u64;
pub type PortType = u64;

pub const DATA: u8 = 0x01;
pub const RECORD_START: u8 = 0x02;
pub const RECORD_STOP: u8 = 0x03;

pub const E_FLAG: u8 = 1 << 7;
pub const M_FLAG: u8 = 1 << 6;
pub const L_FLAG: u8 = 1 << 5;

/// # Data Message
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// |E|M|L|  DATA   | # DATA=0x00
/// +---------------+
/// ~   timestamp   ~
/// +---------------+
/// ~ E2E deadlines ~ if E==1
/// +---------------+
/// ~ E2D dead miss ~ if M==1
/// +---------------+
/// ~   loop ctxs   ~ if L==1
/// +---------------+
/// ~     [u8]      ~
/// +---------------+
/// ```
///

/// # Recording Start Message
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// |X|X|X| RECSTART| # RECSTART=0x01
/// +---------------+
/// ~   timestamp   ~
/// +---------------+
/// ~    PortId     ~ # U64
/// +---------------+
/// ~    NodeId     ~ # U64
/// +---------------+
/// ~    FlowId     ~ # U64
/// +---------------+
/// ~  InstanceId   ~ # [u8,16]
/// +---------------+
/// ```

/// # Recording Stop Message
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// |X|X|X| RECSTOP | # RECSTOP=0x02
/// +---------------+
/// ~   timestamp   ~
/// +---------------+
/// ```

#[derive(Clone)]
pub enum ZenohFlowMessage {
    Data(DataMessage),
    RecordStart(RecordingStart),
    RecordStop(RecordingStop),
}

impl ZenohFlowMessage {
    #[inline(always)]
    pub fn serialize(&self, buff: &mut WBuf) -> bool {
        match &self {
            Self::Data(d) => d.serialize_custom(buff),
            Self::RecordStart(d) => d.serialize_custom(buff),
            Self::RecordStop(d) => d.serialize_custom(buff),
        }
    }
}

#[derive(Clone)]
pub struct RecordingStop(Timestamp);
impl RecordingStop {
    pub fn new(ts: Timestamp) -> Self {
        Self(ts)
    }
}

impl ZFMessage for RecordingStop {
    #[inline(always)]
    fn serialize_custom(&self, buff: &mut WBuf) -> bool {
        buff.write(RECORD_STOP) && buff.write_timestamp(&self.0)
    }
}

#[derive(Clone)]
pub struct RecordingStart {
    pub ts: Timestamp,
    pub port_id: PortId,
    pub node_id: NodeId,
    pub flow_id: FlowId,
    pub instance_id: Uuid,
}

impl RecordingStart {
    pub fn new(
        ts: Timestamp,
        port_id: PortId,
        node_id: NodeId,
        flow_id: FlowId,
        instance_id: Uuid,
    ) -> Self {
        Self {
            ts,
            port_id,
            node_id,
            flow_id,
            instance_id,
        }
    }
}

impl ZFMessage for RecordingStart {
    #[inline(always)]
    fn serialize_custom(&self, buff: &mut WBuf) -> bool {
        buff.write(RECORD_START)
            && buff.write_timestamp(&self.ts)
            && buff.write_bytes(self.port_id.to_le_bytes().as_slice())
            && buff.write_bytes(self.node_id.to_le_bytes().as_slice())
            && buff.write_bytes(self.flow_id.to_le_bytes().as_slice())
            && buff.write_bytes_array(self.instance_id.as_bytes())
    }
}

// Timestamp is uhlc::HLC::Timestamp (u64, usize, [u8;16])
// All the Ids are Arc<str>
// Uuid is uuid::Uuid ([u8;16])
// Duration is std::Duration (u64,u32)

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub(crate) data: Data,
    pub(crate) timestamp: Timestamp,
    pub(crate) end_to_end_deadlines: Vec<E2EDeadline>,
    pub(crate) missed_end_to_end_deadlines: Vec<E2EDeadlineMiss>,
    pub(crate) loop_contexts: Vec<LoopContext>,
}

impl DataMessage {
    pub fn new(data: Data, timestamp: Timestamp, end_to_end_deadlines: Vec<E2EDeadline>) -> Self {
        Self {
            data,
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
            loop_contexts: vec![],
        }
    }

    /// Returns a mutable reference over the Data representation (i.e. `Bytes` or `Typed`).
    ///
    /// This method should be called in conjonction with `try_get::<Typed>()` in order to get a
    /// reference of the desired type. For instance:
    ///
    /// `let zf_usise: &ZFUsize = data_message.get_inner_data().try_get::<ZFUsize>()?;`
    ///
    /// Note that the prerequisite for the above code to work is that `ZFUsize` implements the
    /// traits: `ZFData` and `Deserializable`.
    pub fn get_inner_data(&mut self) -> &mut Data {
        &mut self.data
    }

    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    pub fn get_missed_end_to_end_deadlines(&self) -> &[E2EDeadlineMiss] {
        self.missed_end_to_end_deadlines.as_slice()
    }

    pub fn get_loop_contexts(&self) -> &[LoopContext] {
        self.loop_contexts.as_slice()
    }

    pub fn new_serialized(
        data: Arc<Vec<u8>>,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
        loop_contexts: Vec<LoopContext>,
    ) -> Self {
        Self {
            data: Data::Bytes(data),
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
            loop_contexts,
        }
    }

    pub fn new_deserialized(
        data: Arc<dyn ZFData>,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
        loop_contexts: Vec<LoopContext>,
    ) -> Self {
        Self {
            data: Data::Typed(data),
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
            loop_contexts,
        }
    }
}

impl ZFMessage for DataMessage {
    #[inline(always)]
    fn serialize_custom(&self, buff: &mut WBuf) -> bool {
        let mut header = DATA;
        if !self.end_to_end_deadlines.is_empty() {
            header |= E_FLAG;
        }
        if !self.missed_end_to_end_deadlines.is_empty() {
            header |= M_FLAG;
        }
        if !self.loop_contexts.is_empty() {
            header |= L_FLAG;
        }

        let data_bytes = self.data.try_as_bytes().unwrap();

        buff.write(header)
            && buff.write_timestamp(&self.timestamp)
            && buff.write_usize_as_zint(data_bytes.len())
            && buff.write_bytes(&data_bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    // Typed data is never serialized
    Typed(Arc<dyn ZFData>),
}

impl Data {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::Bytes(Arc::new(bytes))
    }

    pub fn try_as_bytes(&self) -> ZFResult<Arc<Vec<u8>>> {
        match &self {
            Self::Bytes(bytes) => Ok(bytes.clone()),
            Self::Typed(typed) => {
                let serialized_data = typed
                    .try_serialize()
                    .map_err(|_| ZFError::SerializationError)?;
                Ok(Arc::new(serialized_data))
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct E2EDeadline {
    pub duration: Duration,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub start: Timestamp,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct E2EDeadlineMiss {
    pub duration: Duration,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub start: Timestamp,
    pub end: Timestamp,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoopContext {
    pub(crate) ingress: NodeId,
    pub(crate) egress: NodeId,
    pub(crate) iteration: LoopIteration,
    pub(crate) timestamp_start_first_iteration: Timestamp,
    pub(crate) timestamp_start_current_iteration: Option<Timestamp>,
    pub(crate) duration_last_iteration: Option<Duration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LoopIteration {
    Finite(u64),
    Infinite,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputDescriptor {
    pub node: NodeId,
    pub input: PortId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputDescriptor {
    pub node: NodeId,
    pub output: PortId,
}

pub trait ZFMessage {
    fn serialize_custom(&self, buff: &mut WBuf) -> bool;
}

pub fn deserialize_custom(data: &mut ZBuf) -> ZFResult<ZenohFlowMessage> {
    let header = data.read().ok_or(ZFError::DeseralizationError)?;

    if header & DATA == DATA {
        let _e = header & E_FLAG;
        let _m = header & M_FLAG;
        let _l = header & L_FLAG;
        let ts = data.read_timestamp().ok_or(ZFError::DeseralizationError)?;
        let payload = Arc::new(
            data.read_bytes_array()
                .ok_or(ZFError::DeseralizationError)?,
        );

        return Ok(ZenohFlowMessage::Data(DataMessage::new_serialized(
            payload,
            ts,
            vec![],
            vec![],
        )));
    }
    if header & RECORD_START == RECORD_START {
        return Err(ZFError::DeseralizationError);
    }
    if header & RECORD_STOP == RECORD_STOP {
        return Err(ZFError::DeseralizationError);
    }
    Err(ZFError::DeseralizationError)
}

#[cfg(test)]
mod tests {
    use crate::runtime::message_new::{
        deserialize_custom, Data as NewData, DataMessage as NewDataMessage, ZenohFlowMessage, DATA,
    };
    use crate::ZFError;
    use async_std::sync::Arc;
    use zenoh::net::protocol::io::*;
    #[test]
    fn test_ser_de() {
        let hlc = Arc::new(uhlc::HLC::default());
        let payload_data: Vec<u8> = vec![];
        let ts = hlc.new_timestamp();
        let data_new = NewData::from_bytes(payload_data.clone());
        let data_msg_new = NewDataMessage::new(data_new, ts, vec![]);
        let zf_msg_new = ZenohFlowMessage::Data(data_msg_new);

        let mut wbuff = WBuf::new(256, false);
        zf_msg_new.serialize(&mut wbuff);

        let mut zbuf: ZBuf = wbuff.into();

        let header = zbuf.read().ok_or(ZFError::DeseralizationError).unwrap();

        assert_eq!(header, DATA);

        assert_eq!(header & DATA, DATA);

        let r_ts = zbuf
            .read_timestamp()
            .ok_or_else(|| ZFError::InvalidData(String::from("Unable to get ts")))
            .unwrap();

        assert_eq!(r_ts, ts);

        let r_payload = Arc::new(
            zbuf.read_bytes_array()
                .ok_or_else(|| ZFError::InvalidData(String::from("Unable to get payload")))
                .unwrap(),
        );

        assert_eq!(payload_data, *r_payload);

        zbuf.reset();

        let de = deserialize_custom(&mut zbuf).is_ok();

        assert!(de);
    }
}
