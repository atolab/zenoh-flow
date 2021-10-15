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

use crate::async_std::sync::{Arc, RwLock};
use crate::model::node::SinkRecord;
use crate::runtime::graph::link::LinkReceiver;
use crate::runtime::message::{DataMessage, Message};
use crate::runtime::runners::RunAction;
use crate::types::ZFResult;
use crate::{Context, OperatorId, PortId, Sink, State};
use futures::future;
use libloading::Library;
use std::collections::HashMap;

pub type SinkRegisterFn = fn() -> ZFResult<Arc<dyn Sink>>;

pub struct SinkDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: SinkRegisterFn,
}

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the sink/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct SinkRunner {
    pub record: Arc<SinkRecord>,
    pub state: Arc<RwLock<Box<dyn State>>>,
    pub inputs: Arc<RwLock<HashMap<OperatorId, LinkReceiver<Message>>>>,
    pub sink: Arc<dyn Sink>,
    pub lib: Arc<Option<Library>>,
}

impl SinkRunner {
    pub fn new(record: SinkRecord, sink: Arc<dyn Sink>, lib: Option<Library>) -> Self {
        let state = sink.initialize(&record.configuration);
        Self {
            record: Arc::new(record),
            state: Arc::new(RwLock::new(state)),
            inputs: Arc::new(RwLock::new(HashMap::new())),
            sink,
            lib: Arc::new(lib),
        }
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>, from_id: OperatorId) {
        log::trace!("add_input({:?},{:?}", input, from_id);
        self.inputs.write().await.insert(from_id, input);
    }

    pub async fn remove_input(&self, port_id: PortId, from_id: OperatorId) {
        log::trace!("remove_input({:?},{:?}", port_id, from_id);
        let mut inputs = self.inputs.write().await;
        if let Some(input) = inputs.remove(&from_id) {
            if input.id() != port_id {
                log::warn!(
                    "Unable to remove link from port {:?} from node {:?}: port not found",
                    port_id,
                    from_id
                );
            }
        } else {
            log::warn!(
                "Unable to remove link from port {:?} from node {:?}: from not found",
                port_id,
                from_id
            );
        }
    }

    pub async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.sink.clean(&mut state)
    }

    pub async fn run(&self) -> ZFResult<RunAction> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let inputs = self.inputs.read().await;
            let mut state = self.state.write().await;

            let links: Vec<_> = inputs.values().map(|rx| rx.recv()).collect();

            // FEAT. With the introduction of deadline, a DeadlineMissToken could be sent.
            let input: DataMessage;

            match future::select_all(links).await {
                (Ok((_, message)), _, _) => match message.as_ref() {
                    Message::Data(d) => input = d.clone(),
                    Message::Control(_) => todo!(),
                },
                (Err(e), index, _) => {
                    log::error!("[Link] Received an error on < {} >: {:?}.", index, e);
                    continue;
                }
            }

            self.sink.run(&mut context, &mut state, input).await?;
        }
    }
}
