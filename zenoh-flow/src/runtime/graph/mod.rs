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

pub mod link;
pub mod node;

use crate::{Operator, Sink, Source};
use async_std::sync::Arc;
use node::DataFlowNode;
use petgraph::dot::{Config, Dot};
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use std::collections::HashMap;
use std::convert::TryFrom;
use uhlc::HLC;

use crate::runtime::loader::ComponentLoader;
use crate::runtime::message::Message;
use crate::runtime::runners::connector::{ZenohReceiver, ZenohSender};
use crate::runtime::runners::{
    operator::OperatorRunner, sink::SinkRunner, source::SourceRunner, Runner,
};
use crate::{
    model::component::{OperatorRecord, SinkRecord, SourceRecord},
    model::connector::ZFConnectorKind,
    model::dataflow::DataFlowRecord,
    model::link::{LinkDescriptor, LinkFromDescriptor, LinkToDescriptor, PortDescriptor},
    runtime::graph::link::link,
    runtime::graph::node::DataFlowNodeKind,
    types::{OperatorId, ZFError, ZFResult},
    utils::hlc::PeriodicHLC,
};
use uuid::Uuid;

pub struct DataFlowGraph {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: Vec<(NodeIndex, DataFlowNode)>,
    pub links: Vec<(EdgeIndex, LinkDescriptor)>,
    pub graph: StableGraph<DataFlowNode, (String, String)>,
    pub operators_runners: HashMap<OperatorId, (Runner, DataFlowNodeKind)>,
    pub loader: Arc<ComponentLoader>,
}

impl DataFlowGraph {
    pub fn new(loader: Arc<ComponentLoader>) -> Self {
        Self {
            uuid: Uuid::nil(),
            flow: "".to_string(),
            operators: Vec::new(),
            links: Vec::new(),
            graph: StableGraph::<DataFlowNode, (String, String)>::new(),
            operators_runners: HashMap::new(),
            loader,
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.flow = name;
    }

    pub fn to_dot_notation(&self) -> String {
        format!(
            "{:?}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        )
    }

    pub fn add_static_operator(
        &mut self,
        hlc: Arc<HLC>,
        id: OperatorId,
        inputs: Vec<PortDescriptor>,
        outputs: Vec<PortDescriptor>,
        operator: Arc<dyn Operator>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let record = OperatorRecord {
            id: id.clone(),
            inputs,
            outputs,
            uri: None,
            configuration,
            runtime: "self".into(),
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Operator(record.clone())),
            DataFlowNode::Operator(record.clone()),
        ));
        let runner = Runner::Operator(OperatorRunner::new(record, hlc, operator, None));
        self.operators_runners
            .insert(id, (runner, DataFlowNodeKind::Operator));
        Ok(())
    }

    pub fn add_static_source(
        &mut self,
        hlc: Arc<HLC>,
        id: OperatorId,
        output: PortDescriptor,
        source: Arc<dyn Source>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let record = SourceRecord {
            id: id.clone(),
            output,
            period: None,
            uri: None,
            configuration,
            runtime: "self".into(),
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Source(record.clone())),
            DataFlowNode::Source(record.clone()),
        ));
        let non_periodic_hlc = PeriodicHLC::new(hlc, None);
        let runner = Runner::Source(SourceRunner::new(record, non_periodic_hlc, source, None));
        self.operators_runners
            .insert(id, (runner, DataFlowNodeKind::Source));
        Ok(())
    }

    pub fn add_static_sink(
        &mut self,
        id: OperatorId,
        input: PortDescriptor,
        sink: Arc<dyn Sink>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let record = SinkRecord {
            id: id.clone(),
            input,
            uri: None,
            configuration,
            runtime: "self".into(),
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Sink(record.clone())),
            DataFlowNode::Sink(record.clone()),
        ));
        let runner = Runner::Sink(SinkRunner::new(record, sink, None));
        self.operators_runners
            .insert(id, (runner, DataFlowNodeKind::Sink));
        Ok(())
    }

    pub fn add_link(
        &mut self,
        from: LinkFromDescriptor,
        to: LinkToDescriptor,
        size: Option<usize>,
        queueing_policy: Option<String>,
        priority: Option<usize>,
    ) -> ZFResult<()> {
        let connection = LinkDescriptor {
            from,
            to,
            size,
            queueing_policy,
            priority,
        };

        let (from_index, from_type) = match self
            .operators
            .iter()
            .find(|&(_, o)| o.get_id() == connection.from.component.clone())
        {
            Some((idx, op)) => match op.has_output(connection.from.output.clone()) {
                true => (idx, op.get_output_type(connection.from.output.clone())?),
                false => {
                    return Err(ZFError::PortNotFound((
                        connection.from.component.clone(),
                        connection.from.output.clone(),
                    )))
                }
            },
            None => return Err(ZFError::OperatorNotFound(connection.from.component.clone())),
        };

        let (to_index, to_type) = match self
            .operators
            .iter()
            .find(|&(_, o)| o.get_id() == connection.to.component.clone())
        {
            Some((idx, op)) => match op.has_input(connection.to.input.clone()) {
                true => (idx, op.get_input_type(connection.to.input.clone())?),
                false => {
                    return Err(ZFError::PortNotFound((
                        connection.to.component.clone(),
                        connection.to.input.clone(),
                    )))
                }
            },
            None => return Err(ZFError::OperatorNotFound(connection.to.component.clone())),
        };

        if from_type == to_type {
            self.links.push((
                self.graph.add_edge(
                    *from_index,
                    *to_index,
                    (connection.from.output.clone(), connection.to.input.clone()),
                ),
                connection,
            ));
            return Ok(());
        }

        Err(ZFError::PortTypeNotMatching((
            String::from(from_type),
            String::from(to_type),
        )))
    }

    pub async fn load(&mut self, runtime: &str, session: Arc<zenoh::net::Session>) -> ZFResult<()> {
        let hlc = Arc::new(uhlc::HLC::default());

        for (_, op) in &self.operators {
            if op.get_runtime().as_ref() != runtime {
                continue;
            }

            match op {
                DataFlowNode::Operator(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner = self
                                .loader
                                .load_operator(inner.clone(), hlc.clone(), uri.clone())
                                .await?;
                            let runner = Runner::Operator(runner);
                            self.operators_runners
                                .insert(inner.id.clone(), (runner, DataFlowNodeKind::Operator));
                        }
                        None => {
                            // this is a static operator.
                        }
                    }
                }
                DataFlowNode::Source(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner = self
                                .loader
                                .load_source(
                                    inner.clone(),
                                    PeriodicHLC::new(hlc.clone(), inner.period.clone()),
                                    uri.clone(),
                                )
                                .await?;
                            let runner = Runner::Source(runner);
                            self.operators_runners
                                .insert(inner.id.clone(), (runner, DataFlowNodeKind::Source));
                        }
                        None => {
                            // static source
                        }
                    }
                }
                DataFlowNode::Sink(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner = self.loader.load_sink(inner.clone(), uri.clone()).await?;
                            let runner = Runner::Sink(runner);
                            self.operators_runners
                                .insert(inner.id.clone(), (runner, DataFlowNodeKind::Sink));
                        }
                        None => {
                            //static sink
                        }
                    }
                }
                DataFlowNode::Connector(zc) => match zc.kind {
                    ZFConnectorKind::Sender => {
                        let runner = ZenohSender::new(session.clone(), zc.resource.clone(), None);
                        let runner = Runner::Sender(runner);
                        self.operators_runners
                            .insert(zc.id.clone(), (runner, DataFlowNodeKind::Connector));
                    }

                    ZFConnectorKind::Receiver => {
                        let runner = ZenohReceiver::new(session.clone(), zc.resource.clone(), None);
                        let runner = Runner::Receiver(runner);
                        self.operators_runners
                            .insert(zc.id.clone(), (runner, DataFlowNodeKind::Connector));
                    }
                },
            }
        }
        Ok(())
    }

    pub async fn make_connections(&mut self, runtime: &str) -> ZFResult<()> {
        // Connects the operators via our FIFOs

        for (idx, up_op) in &self.operators {
            if up_op.get_runtime().as_ref() != runtime {
                continue;
            }

            log::debug!("Creating links for:\n\t< {:?} > Operator: {:?}", idx, up_op);

            let (up_runner, _) = self
                .operators_runners
                .get(&up_op.get_id())
                .ok_or_else(|| ZFError::OperatorNotFound(up_op.get_id()))?;
            // let mut up_runner = up_runner.lock().await;

            if self.graph.contains_node(*idx) {
                let mut downstreams = self
                    .graph
                    .neighbors_directed(*idx, Direction::Outgoing)
                    .detach();
                while let Some((down_edge_index, down_node_index)) = downstreams.next(&self.graph) {
                    let (_link_index, _down_link) = self
                        .links
                        .iter()
                        .find(|&(edge_index, _)| *edge_index == down_edge_index)
                        .ok_or_else(|| ZFError::OperatorNotFound(up_op.get_id()))?;

                    let (link_id_from, link_id_to) = match self.graph.edge_weight(down_edge_index) {
                        Some(w) => w,
                        None => return Err(ZFError::GenericError),
                    };

                    let down_op = match self
                        .operators
                        .iter()
                        .find(|&(idx, _)| *idx == down_node_index)
                    {
                        Some((_, op)) => op,
                        None => panic!("To not found"),
                    };

                    let (down_runner, _) = self
                        .operators_runners
                        .get(&down_op.get_id())
                        .ok_or_else(|| ZFError::OperatorNotFound(down_op.get_id()))?;

                    log::debug!(
                        "\t Creating link between {:?} -> {:?}: {:?} -> {:?}",
                        idx,
                        down_node_index,
                        link_id_from,
                        link_id_to,
                    );
                    let (tx, rx) =
                        link::<Message>(None, String::from(link_id_from), String::from(link_id_to));

                    up_runner.add_output(tx).await?;
                    down_runner.add_input(rx).await?;
                }
            }
        }
        Ok(())
    }

    pub fn get_runner(&self, operator_id: &OperatorId) -> Option<&Runner> {
        self.operators_runners.get(operator_id).map(|(r, _)| r)
    }

    pub fn get_runners(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, _) in self.operators_runners.values() {
            runners.push(runner);
        }
        runners
    }

    pub fn get_sources(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Source = kind {
                runners.push(runner);
            }
        }
        runners
    }

    pub fn get_sinks(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Sink = kind {
                runners.push(runner);
            }
        }
        runners
    }

    pub fn get_operators(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Operator = kind {
                runners.push(runner);
            }
        }
        runners
    }

    pub fn get_connectors(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Connector = kind {
                runners.push(runner);
            }
        }
        runners
    }
}

impl TryFrom<(DataFlowRecord, Arc<ComponentLoader>)> for DataFlowGraph {
    type Error = ZFError;

    fn try_from(dr: (DataFlowRecord, Arc<ComponentLoader>)) -> Result<Self, Self::Error> {
        let (dr, loader) = dr;
        let mut graph = StableGraph::<DataFlowNode, (String, String)>::new();
        let mut operators = Vec::new();
        let mut links: Vec<(EdgeIndex, LinkDescriptor)> = Vec::new();
        for o in dr.operators {
            operators.push((
                graph.add_node(DataFlowNode::Operator(o.clone())),
                DataFlowNode::Operator(o),
            ));
        }

        for o in dr.sources {
            operators.push((
                graph.add_node(DataFlowNode::Source(o.clone())),
                DataFlowNode::Source(o),
            ));
        }

        for o in dr.sinks {
            operators.push((
                graph.add_node(DataFlowNode::Sink(o.clone())),
                DataFlowNode::Sink(o),
            ));
        }

        for o in dr.connectors {
            operators.push((
                graph.add_node(DataFlowNode::Connector(o.clone())),
                DataFlowNode::Connector(o),
            ));
        }

        for l in dr.links {
            // First check if the LinkId are the same
            // if l.from.output != l.to.input {
            //     return Err(ZFError::PortIdNotMatching((
            //         l.from.output.clone(),
            //         l.to.input,
            //     )));
            // }

            let (from_index, from_runtime, from_type) = match operators
                .iter()
                .find(|&(_, o)| o.get_id() == l.from.component)
            {
                Some((idx, op)) => match op.has_output(l.from.output.clone()) {
                    true => (
                        idx,
                        op.get_runtime(),
                        op.get_output_type(l.from.output.clone())?,
                    ),
                    false => {
                        return Err(ZFError::PortNotFound((
                            l.from.component,
                            l.from.output.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(l.from.component)),
            };

            let (to_index, to_runtime, to_type) = match operators
                .iter()
                .find(|&(_, o)| o.get_id() == l.to.component)
            {
                Some((idx, op)) => match op.has_input(l.to.input.clone()) {
                    true => (
                        idx,
                        op.get_runtime(),
                        op.get_input_type(l.to.input.clone())?,
                    ),
                    false => {
                        return Err(ZFError::PortNotFound((l.to.component, l.to.input.clone())))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(l.to.component)),
            };

            if to_type != from_type {
                return Err(ZFError::PortTypeNotMatching((
                    to_type.to_string(),
                    from_type.to_string(),
                )));
            }

            if from_runtime == to_runtime {
                log::debug!("[Graph instantiation] [same runtime] Pushing link: {:?}", l);
                links.push((
                    graph.add_edge(
                        *from_index,
                        *to_index,
                        (l.from.output.clone(), l.to.input.clone()),
                    ),
                    l.clone(),
                ));
            } else {
                log::debug!(
                    "[Graph instantiation] Link on different runtime detected: {:?}, this should not happen! :P",
                    l
                );

                // We do nothing in this case... the links are already well created when creating the record, so this should NEVER happen
            }
        }

        Ok(Self {
            uuid: dr.uuid,
            flow: dr.flow,
            operators,
            links,
            graph,
            operators_runners: HashMap::new(),
            loader,
        })
    }
}
