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
use crate::model::node::OperatorRecord;
use crate::runtime::graph::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::runtime::runners::RunAction;
use crate::types::{Token, ZFResult};
use crate::{Context, Operator, OperatorId, PortId, State};
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use uhlc::HLC;

pub type OperatorRegisterFn = fn() -> ZFResult<Arc<dyn Operator>>;

pub struct OperatorDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: OperatorRegisterFn,
}

pub type OperatorIO = (
    HashMap<OperatorId, LinkReceiver<Message>>,
    HashMap<PortId, HashMap<OperatorId, LinkSender<Message>>>,
);

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the operator/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct OperatorRunner {
    pub record: Arc<OperatorRecord>,
    pub io: Arc<RwLock<OperatorIO>>,
    pub state: Arc<RwLock<Box<dyn State>>>,
    pub hlc: Arc<HLC>,
    pub operator: Arc<dyn Operator>,
    pub lib: Arc<Option<Library>>,
}

impl OperatorRunner {
    pub fn new(
        record: OperatorRecord,
        hlc: Arc<HLC>,
        operator: Arc<dyn Operator>,
        lib: Option<Library>,
    ) -> Self {
        let state = operator.initialize(&record.configuration);
        Self {
            record: Arc::new(record),
            hlc,
            io: Arc::new(RwLock::new((HashMap::new(), HashMap::new()))),
            state: Arc::new(RwLock::new(state)),
            operator,
            lib: Arc::new(lib),
        }
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>, from_id: OperatorId) {
        self.io.write().await.0.insert(from_id, input);
    }

    pub async fn remove_input(&self, port_id: PortId, from_id: OperatorId) {
        let mut guard = self.io.write().await;
        if let Some(input) = guard.0.remove(&from_id) {
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

    pub async fn add_output(&self, output: LinkSender<Message>, to_id: OperatorId) {
        let mut guard = self.io.write().await;

        let key = output.id();
        if let Some(links) = guard.1.get_mut(&key) {
            links.insert(to_id, output);
        } else {
            let mut link = HashMap::new();
            link.insert(to_id, output);
            guard.1.insert(key, link);
        }
    }

    pub async fn remove_output(&self, port_id: PortId, to_id: OperatorId) {
        let mut guard = self.io.write().await;
        if let Some(links) = guard.1.get_mut(&port_id) {
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
        self.operator.clean(&mut state)
    }

    pub async fn run(&self) -> ZFResult<RunAction> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow
            // interleaving.
            let io = self.io.read().await;
            let mut state = self.state.write().await;

            // we should start from an HashMap with all PortId and not ready tokens
            let mut msgs: HashMap<PortId, Token> = HashMap::new();

            for (_, i) in io.0.iter() {
                msgs.insert(i.id(), Token::NotReady);
            }

            let mut futs = vec![];
            for rx in io.0.values() {
                futs.push(rx.recv()); // this should be peek()
            }

            // Input Rules
            crate::run_input_rules!(self.operator, msgs, futs, &mut state, &mut context);

            let mut data = HashMap::with_capacity(msgs.len());
            let mut max_token_timestamp = None;

            for (id, token) in msgs {
                // Keep the biggest timestamp (i.e. most recent) associated to the inputs. This
                // timestamp will be reported to all outputs and used to update the HLC.
                let token_timestamp = token.get_timestamp();
                max_token_timestamp = match (&max_token_timestamp, &token_timestamp) {
                    (None, _) => token_timestamp,
                    (Some(_), None) => max_token_timestamp,
                    (Some(max_time), Some(time)) => {
                        if max_time < time {
                            token_timestamp
                        } else {
                            max_token_timestamp
                        }
                    }
                };

                let (d, _) = token.split();
                data.insert(id, d.unwrap());
            }

            let timestamp = {
                match max_token_timestamp {
                    Some(max_timestamp) => {
                        if let Err(error) = self.hlc.update_with_timestamp(&max_timestamp) {
                            log::warn!(
                                "[HLC] Could not update HLC with timestamp {:?}: {:?}",
                                max_timestamp,
                                error
                            );
                        }

                        max_timestamp
                    }
                    None => self.hlc.new_timestamp(),
                }
            };

            // Running
            let run_outputs = self.operator.run(&mut context, &mut state, &mut data)?;

            // Output rules
            let outputs = self
                .operator
                .output_rule(&mut context, &mut state, run_outputs)?;

            // Send to Links
            for (id, output) in outputs {
                // getting link
                log::trace!("id: {:?}, message: {:?}", id, output);
                if let Some(links) = io.1.get(&id) {
                    let zf_message = Arc::new(Message::from_node_output(output, timestamp));

                    for (to, tx) in links {
                        log::trace!("Sending to {:?} on: {:?}", to, tx);
                        match tx.send(zf_message.clone()).await {
                            Ok(_) => (),
                            Err(e) => {
                                log::warn!("Error when sending to {:?} on {:?}: {:?}", to, tx, e)
                            }
                        }
                    }
                }
            }

            // This depends on the Tokens...
            for (_, rx) in io.0.iter() {
                rx.discard().await?;
            }
        }
    }
}
