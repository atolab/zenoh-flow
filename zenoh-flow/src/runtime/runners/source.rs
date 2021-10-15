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
#![allow(clippy::type_complexity)]

use crate::async_std::sync::{Arc, RwLock};
use crate::model::node::SourceRecord;
use crate::runtime::graph::link::LinkSender;
use crate::runtime::message::Message;
use crate::runtime::runners::RunAction;
use crate::types::ZFResult;
use crate::utils::hlc::PeriodicHLC;
use crate::OperatorId;
use crate::{Context, PortId, Source, State};
use libloading::Library;
use std::collections::HashMap;

pub type SourceRegisterFn = fn() -> ZFResult<Arc<dyn Source>>;

pub struct SourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: SourceRegisterFn,
}

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the source/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct SourceRunner {
    pub record: Arc<SourceRecord>,
    pub hlc: Arc<PeriodicHLC>,
    pub state: Arc<RwLock<Box<dyn State>>>,
    pub outputs: Arc<RwLock<HashMap<PortId, HashMap<OperatorId, LinkSender<Message>>>>>,
    pub source: Arc<dyn Source>,
    pub lib: Arc<Option<Library>>,
}

impl SourceRunner {
    pub fn new(
        record: SourceRecord,
        hlc: PeriodicHLC,
        source: Arc<dyn Source>,
        lib: Option<Library>,
    ) -> Self {
        let state = source.initialize(&record.configuration);
        Self {
            record: Arc::new(record),
            hlc: Arc::new(hlc),
            state: Arc::new(RwLock::new(state)),
            outputs: Arc::new(RwLock::new(HashMap::new())),
            source,
            lib: Arc::new(lib),
        }
    }

    pub async fn add_output(&self, output: LinkSender<Message>, to_id: OperatorId) {
        log::trace!("add_output({:?},{:?}", output, to_id);
        let mut outputs = self.outputs.write().await;
        let key = output.id();
        if let Some(links) = outputs.get_mut(&key) {
            links.insert(to_id, output);
        } else {
            let mut link = HashMap::new();
            link.insert(to_id, output);
            outputs.insert(key, link);
        }
    }

    pub async fn remove_output(&self, port_id: PortId, to_id: OperatorId) {
        log::trace!("remove_output({:?},{:?}", port_id, to_id);
        let mut outputs = self.outputs.write().await;
        if let Some(links) = outputs.get_mut(&port_id) {
            if links.remove(&to_id).is_none() {
                log::warn!(
                    "Unable to remove link from port {:?} to node {:?}: to not found",
                    port_id,
                    to_id
                );
            }
        } else {
            log::warn!(
                "Unable to remove link from port {:?} to node {:?}: port not found",
                port_id,
                to_id
            );
        }
    }

    pub async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.source.clean(&mut state)
    }

    pub async fn run(&self) -> ZFResult<RunAction> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let outputs_links = self.outputs.read().await;
            let mut state = self.state.write().await;

            // Running
            let (id, output) = self.source.run(&mut context, &mut state).await?;

            let timestamp = self.hlc.new_timestamp();

            // Send to Links
            log::trace!("Sending on {:?} data: {:?}", id, output);

            if let Some(links) = outputs_links.get(&id) {
                let zf_message = Arc::new(Message::from_serdedata(output, timestamp));

                for (to, tx) in links {
                    log::trace!("Sending to {:?} on: {:?}", to, tx);
                    match tx.send(zf_message.clone()).await {
                        Ok(_) => (),
                        Err(e) => log::warn!("Error when sending to {:?} on {:?}: {:?}", to, tx, e),
                    }
                }
            }
        }
    }
}
