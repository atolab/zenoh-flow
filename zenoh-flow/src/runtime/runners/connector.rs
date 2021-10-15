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

use crate::async_std::sync::{Arc, Mutex, RwLock};
use crate::async_std::task;
use crate::runtime::graph::link::{LinkReceiver, LinkSender};
use crate::runtime::message::{ControlMessage, Message};
use crate::runtime::runners::RunAction;
use crate::{OperatorId, ZFError, ZFResult};
use futures::future;
use futures::prelude::*;
use std::collections::HashMap;
use uhlc::{Timestamp, HLC};
use zenoh::net::*;

#[derive(Clone)]
pub struct ZenohSender {
    pub session: Arc<Session>,
    pub resource: String,
    pub inputs: Arc<RwLock<HashMap<OperatorId, LinkReceiver<Message>>>>,
    pub hlc: Arc<HLC>,
}

impl ZenohSender {
    pub fn new(
        session: Arc<Session>,
        resource: String,
        input: Option<(OperatorId, LinkReceiver<Message>)>,
        hlc: Arc<HLC>,
    ) -> Self {
        let inputs = match input {
            Some((from, rx)) => {
                let mut inputs = HashMap::new();
                inputs.insert(from, rx);
                Arc::new(RwLock::new(inputs))
            }
            None => Arc::new(RwLock::new(HashMap::new())),
        };

        Self {
            session,
            resource,
            inputs,
            hlc,
        }
    }

    pub async fn run(&self) -> ZFResult<RunAction> {
        log::debug!("ZenohSender - {} - Started", self.resource);
        loop {
            let guard = self.inputs.read().await;
            let links: Vec<_> = guard.values().map(|rx| rx.recv()).collect();
            match links.len() {
                0 => (),
                _ => {
                    match future::select_all(links).await {
                        (Ok((_, message)), _, _) => {
                            log::trace!("ZenohSender IN <= {:?} ", message);
                            let serialized = message.serialize_bincode()?;
                            log::trace!("ZenohSender - {}=>{:?} ", self.resource, serialized);
                            self.session
                                .write(&self.resource.clone().into(), serialized.into())
                                .await?;
                        }
                        (Err(e), index, _) => {
                            log::error!("[Link] Received an error on < {} >: {:?}.", index, e);
                            continue;
                        }
                    };
                }
            }
        }
    }

    pub async fn start_recording(&self) -> ZFResult<()> {
        let ts_recoding_start = self.hlc.new_timestamp();
        let message = Message::Control(ControlMessage::RecordingStart(ts_recoding_start));
        let serialized = message.serialize_bincode()?;
        log::debug!(
            "ZenohSender - {} - Started recoding at {:?}",
            self.resource,
            ts_recoding_start
        );
        Ok(self
            .session
            .write(&self.resource.clone().into(), serialized.into())
            .await?)
    }

    pub async fn stop_recording(&self) -> ZFResult<()> {
        let ts_recoding_stop = self.hlc.new_timestamp();
        let message = Message::Control(ControlMessage::RecordingStop(ts_recoding_stop));
        let serialized = message.serialize_bincode()?;
        log::debug!(
            "ZenohSender - {} - Stop recoding at {:?}",
            self.resource,
            ts_recoding_stop
        );
        Ok(self
            .session
            .write(&self.resource.clone().into(), serialized.into())
            .await?)
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>, from_id: OperatorId) {
        log::trace!("add_input({:?},{:?}", input, from_id);
        self.inputs.write().await.insert(from_id, input);
    }

    pub async fn remove_input(&self, from_id: OperatorId) {
        log::trace!("remove_input({:?}", from_id);
        let mut inputs = self.inputs.write().await;
        if inputs.remove(&from_id).is_none() {
            log::warn!("Unable to remove link from node {:?}: not found", from_id);
        }
    }
}

#[derive(Clone)]
pub struct ZenohReceiver {
    pub session: Arc<Session>,
    pub resource: String,
    pub outputs: Arc<RwLock<HashMap<OperatorId, LinkSender<Message>>>>,
}

impl ZenohReceiver {
    pub fn new(
        session: Arc<Session>,
        resource: String,
        output: Option<(OperatorId, LinkSender<Message>)>,
    ) -> Self {
        let outputs = match output {
            Some((to, tx)) => {
                let mut outputs = HashMap::new();
                outputs.insert(to, tx);
                Arc::new(RwLock::new(outputs))
            }
            None => Arc::new(RwLock::new(HashMap::new())),
        };

        Self {
            session,
            resource,
            outputs,
        }
    }

    pub async fn run(&self) -> ZFResult<RunAction> {
        log::debug!("ZenohReceiver - {} - Started", self.resource);

        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Push,
            period: None,
        };

        let mut subscriber = self
            .session
            .declare_subscriber(&self.resource.clone().into(), &sub_info)
            .await?;

        while let Some(msg) = subscriber.receiver().next().await {
            let guard = self.outputs.read().await;
            log::trace!("ZenohSender - {}<={:?} ", self.resource, msg);
            let de: Arc<Message> = Arc::new(
                bincode::deserialize(&msg.payload.contiguous())
                    .map_err(|_| ZFError::DeseralizationError)?,
            );

            log::trace!("ZenohSender - OUT =>{:?} ", de);
            for (to, tx) in guard.iter() {
                log::trace!("Sending to {:?} on: {:?}", to, tx);
                tx.send(Arc::clone(&de)).await?;
            }
        }
        Ok(RunAction::StopError(ZFError::Disconnected))
    }

    pub async fn add_output(&self, output: LinkSender<Message>, to_id: OperatorId) {
        log::trace!("add_output({:?},{:?}", output, to_id);
        self.outputs.write().await.insert(to_id, output);
    }

    pub async fn remove_output(&self, to_id: OperatorId) {
        log::trace!("remove_output({:?}", to_id);
        let mut outputs = self.outputs.write().await;
        if outputs.remove(&to_id).is_none() {
            log::warn!("Unable to remove link to node {:?}: not found", to_id);
        }
    }
}

#[derive(Clone)]
pub struct ZenohReplay {
    pub session: Arc<Session>,
    pub resource: String,
    pub outputs: Arc<RwLock<HashMap<OperatorId, LinkSender<Message>>>>,
    pub last_ts: Arc<Mutex<Option<Timestamp>>>,
}

impl ZenohReplay {
    pub fn new(
        session: Arc<Session>,
        resource: String,
        output: Option<(OperatorId, LinkSender<Message>)>,
    ) -> Self {
        let outputs = match output {
            Some((to, tx)) => {
                let mut outputs = HashMap::new();
                outputs.insert(to, tx);
                Arc::new(RwLock::new(outputs))
            }
            None => Arc::new(RwLock::new(HashMap::new())),
        };

        Self {
            session,
            resource,
            outputs,
            last_ts: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn run(&self) -> ZFResult<RunAction> {
        log::debug!("ZenohReplay - {} - Started", self.resource);

        let mut replies = self
            .session
            .query(
                &self.resource.clone().into(),
                "",
                QueryTarget {
                    kind: queryable::STORAGE,
                    target: Target::default(),
                },
                QueryConsolidation::default(),
            )
            .await?;

        while let Some(msg) = replies.next().await {
            let mut last_ts = self.last_ts.lock().await;
            let guard = self.outputs.read().await;

            log::trace!("ZenohReplay - {}<={:?} ", self.resource, msg);
            let de: Arc<Message> = Arc::new(
                bincode::deserialize(&msg.data.payload.contiguous())
                    .map_err(|_| ZFError::DeseralizationError)?,
            );

            match &*de {
                Message::Data(d) => {
                    log::trace!("ZenohReplay - OUT =>{:?} ", de);
                    let current_ts = d.timestamp;
                    let sleep = current_ts.get_diff_duration(&(*last_ts).ok_or(ZFError::Empty)?);
                    task::sleep(sleep).await;

                    for (to, tx) in guard.iter() {
                        log::trace!("Sending to {:?} on: {:?}", to, tx);
                        tx.send(Arc::clone(&de)).await?;
                    }
                }
                Message::Control(ctrl) => match ctrl {
                    ControlMessage::RecordingStart(ts) => *last_ts = Some(*ts),
                    ControlMessage::RecordingStop(_ts) => return Ok(RunAction::Stop),
                    _ => return Ok(RunAction::RestartRun(Some(ZFError::Unimplemented))),
                },
            }
        }
        Ok(RunAction::StopError(ZFError::Disconnected))
    }

    pub async fn add_output(&self, output: LinkSender<Message>, to_id: OperatorId) {
        log::trace!("add_output({:?},{:?}", output, to_id);
        self.outputs.write().await.insert(to_id, output);
    }

    pub async fn remove_output(&self, to_id: OperatorId) {
        log::trace!("remove_output({:?}", to_id);
        let mut outputs = self.outputs.write().await;
        if outputs.remove(&to_id).is_none() {
            log::warn!("Unable to remove link to node {:?}: not found", to_id);
        }
    }
}
