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
#![allow(clippy::manual_async_fn)]

use crate::async_std::path::PathBuf;
use crate::async_std::sync::{Arc, Mutex};
use crate::model::RegistryComponentArchitecture;
use crate::types::{LOCAL_REGISTRY_FILE_ROOT, REMOTE_REGISTRY_FILE_ROOT};
use crate::OperatorId;
use crate::{
    model::{
        component::{OperatorDescriptor, SinkDescriptor, SourceDescriptor},
        dataflow::DataFlowDescriptor,
        ComponentKind, RegistryComponent,
    },
    ZFError, ZFResult,
};
use std::path::Path;
use uuid::Uuid;
use zenoh::Path as ZPath;
use zenoh_cdn::client::Client;
use znrpc_macros::{znserver, znservice};
use zrpc::zrpcresult::{ZRPCError, ZRPCResult};
use zrpc::ZNServe;

use zenoh_cdn::server::Server;
use zenoh_cdn::types::ServerConfig;

use futures::prelude::*;
use futures::select;

use zenoh::net::Session as ZSession;
use zenoh::ZFuture;

use crate::runtime::resources::DataStore;
use crate::runtime::ZenohConfig;
use crate::serde::{Deserialize, Serialize};

#[znservice(
    timeout_s = 60,
    prefix = "/zf/registry",
    service_uuid = "00000000-0000-0000-0000-000000000002"
)]
pub trait Registry {
    async fn get_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor>;

    //async fn get_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn get_all_graphs(&self) -> ZFResult<Vec<RegistryComponent>>;

    async fn get_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
        os: String,
        arch: String,
    ) -> ZFResult<OperatorDescriptor>;

    async fn get_sink(&self, sink_id: OperatorId, tag: Option<String>) -> ZFResult<SinkDescriptor>;

    async fn get_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor>;

    async fn remove_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor>;

    // async fn remove_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn remove_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<OperatorDescriptor>;

    async fn remove_sink(
        &self,
        sink_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SinkDescriptor>;

    async fn remove_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor>;

    async fn add_flow(&self, flow: DataFlowDescriptor) -> ZFResult<OperatorId>;

    async fn add_graph(&self, graph: RegistryComponent) -> ZFResult<OperatorId>;

    async fn add_operator(
        &self,
        operator: RegistryComponent,
        tag: Option<String>,
    ) -> ZFResult<OperatorId>;

    async fn add_sink(&self, sink: RegistryComponent, tag: Option<String>) -> ZFResult<OperatorId>;

    async fn add_source(
        &self,
        source: RegistryComponent,
        tag: Option<String>,
    ) -> ZFResult<OperatorId>;
}

#[derive(Clone)]
pub struct RegistryFileClient {
    pub zcdn: Client,
}

impl RegistryFileClient {
    pub fn new(zenoh: Arc<zenoh::Zenoh>, is_local: bool) -> Self {
        match is_local {
            true => {
                let zcdn = Client::new(zenoh, Some(String::from(LOCAL_REGISTRY_FILE_ROOT))); //This should be a constant.
                Self { zcdn }
            }
            false => {
                let zcdn = Client::new(zenoh, Some(String::from(REMOTE_REGISTRY_FILE_ROOT))); //This should be a constant.
                Self { zcdn }
            }
        }
    }

    pub async fn send_component(
        &self,
        path: &Path,
        id: &str,
        arch: &RegistryComponentArchitecture,
        tag: &str,
        kind: &ComponentKind,
    ) -> ZFResult<ZPath> {
        let resource_name = ZPath::try_from(format!(
            "/{}/{}/{}/{}/{}/library",
            kind, id, tag, arch.os, arch.arch
        ))?;
        self.zcdn.upload(path, &resource_name).await?;
        Ok(resource_name)
    }

    pub async fn get_component(&self, component_uri: &ZPath, path: &Path) -> ZFResult<PathBuf> {
        self.zcdn.download(component_uri, path).await?;
        Ok(path.into())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegistryConfig {
    pub pid_file: String,
    pub path: String,
    pub zenoh: ZenohConfig,
    pub chunks_dir: String,
    pub is_local: bool,
}

impl TryFrom<String> for RegistryConfig {
    type Error = ZFError;

    fn try_from(d: String) -> Result<Self, Self::Error> {
        serde_yaml::from_str::<Self>(&d).map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }
}

#[derive(Debug)]
pub struct RegistryState {
    pub config: RegistryConfig,
}

#[derive(Clone)]
pub struct ZFRegistry {
    pub zn: Arc<ZSession>,
    pub store: DataStore,
    pub state: Arc<Mutex<RegistryState>>,
    pub cdn_server: Server,
}

impl ZFRegistry {
    pub fn new(zn: Arc<ZSession>, z: Arc<zenoh::Zenoh>, config: RegistryConfig) -> Self {
        let state = Arc::new(Mutex::new(RegistryState {
            config: config.clone(),
        }));
        let chunks_dir = std::path::PathBuf::from(config.chunks_dir.clone());
        let zcdn_config = match config.is_local {
            true => ServerConfig {
                chunks_dir,
                resource_space: format!("{}/**", LOCAL_REGISTRY_FILE_ROOT),
            },
            false => ServerConfig {
                chunks_dir,
                resource_space: format!("{}/**", REMOTE_REGISTRY_FILE_ROOT),
            },
        };

        Self {
            zn,
            store: DataStore::new(z.clone()),
            state,
            cdn_server: Server::new(z, zcdn_config),
        }
    }

    pub async fn run(&self, stop: async_std::channel::Receiver<()>) -> ZFResult<()> {
        log::info!("Registry main loop starting");

        let reg_server = self.clone().get_registry_server(self.zn.clone(), None);
        let (reg_stopper, _hrt) = reg_server.connect().await?;

        reg_server.initialize().await?;

        reg_server.register().await?;

        log::trace!("Starting Zenoh-CDN Server");
        let _cdn_handler = self.cdn_server.serve();

        log::trace!("Staring ZRPC Servers");
        let (srt, _hrt) = reg_server.start().await?;

        let run_loop = async {
            select!(
                _ = stop.recv().fuse() => {
                    log::info!("Received SIGINT, shutting down");
                    Ok(())
                }
            )
        };

        // let _ = stop
        //     .recv()
        //     .await
        //     .map_err(|e| ZFError::RecvError(format!("{}", e)))?;
        let _: ZFResult<()> = run_loop.await;

        reg_server.stop(srt).await?;

        reg_server.unregister().await?;

        reg_server.disconnect(reg_stopper).await?;

        log::info!("Registry main loop exiting...");
        Ok(())
    }

    pub async fn start(
        &self,
    ) -> ZFResult<(
        async_std::channel::Sender<()>,
        async_std::task::JoinHandle<ZFResult<()>>,
    )> {
        // Starting main loop in a task
        let (s, r) = async_std::channel::bounded::<()>(1);
        let rt = self.clone();

        let h = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async { rt.run(r).await })
        });
        Ok((s, h))
    }

    pub async fn stop(&self, stop: async_std::channel::Sender<()>) -> ZFResult<()> {
        stop.send(())
            .await
            .map_err(|e| ZFError::SendError(format!("{}", e)))?;

        Ok(())
    }
}

impl TryFrom<RegistryConfig> for ZFRegistry {
    type Error = ZFError;

    fn try_from(config: RegistryConfig) -> Result<Self, Self::Error> {
        let zn_properties = zenoh::Properties::from(format!(
            "mode={};peer={};listener={}",
            &config.zenoh.kind,
            &config.zenoh.locators.join(","),
            &config.zenoh.listen.join(",")
        ));

        let zenoh_properties = zenoh::Properties::from(format!(
            "mode={};peer={}",
            &config.zenoh.kind,
            &config.zenoh.locators.join(","),
        ));

        let zn = Arc::new(zenoh::net::open(zn_properties.into()).wait()?);
        let z = Arc::new(zenoh::Zenoh::new(zenoh_properties.into()).wait()?);

        Ok(Self::new(zn, z, config))
    }
}

#[znserver]
impl Registry for ZFRegistry {
    async fn get_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor> {
        Err(ZFError::Unimplemented)
    }

    //async fn get_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn get_all_graphs(&self) -> ZFResult<Vec<RegistryComponent>> {
        self.store.get_all_graphs().await
    }

    async fn get_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
        os: String,
        arch: String,
    ) -> ZFResult<OperatorDescriptor> {
        let metadata = self.store.get_graph(&operator_id).await?;

        let tag = tag.unwrap_or_else(|| String::from("latest"));

        let filtered_metadata = || {
            for m_tag in metadata.clone().tags {
                if m_tag.name == tag {
                    for m_arch in m_tag.architectures {
                        if m_arch.os == os && m_arch.arch == arch {
                            return Ok(m_arch);
                        }
                    }
                }
            }
            Err(ZFError::OperatorNotFound(operator_id))
        };

        let specific_metadata = filtered_metadata()?;

        let descriptor = OperatorDescriptor {
            id: metadata.id.clone(),
            inputs: metadata.inputs.clone(),
            outputs: metadata.outputs.clone(),
            uri: Some(specific_metadata.uri),
            configuration: None,
            runtime: None,
        };

        Ok(descriptor)
    }

    async fn get_sink(&self, sink_id: OperatorId, tag: Option<String>) -> ZFResult<SinkDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn get_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor> {
        Err(ZFError::Unimplemented)
    }

    // async fn remove_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn remove_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<OperatorDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_sink(
        &self,
        sink_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SinkDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn add_flow(&self, flow: DataFlowDescriptor) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }

    async fn add_graph(&self, graph: RegistryComponent) -> ZFResult<OperatorId> {
        log::info!("Adding graph {:?}", graph);
        match self.store.get_graph(&graph.id).await {
            Ok(mut metadata) => {
                // This is an update of something that is already there.
                for tag in graph.tags.into_iter() {
                    metadata.add_tag(tag);
                }
                self.store.add_graph(&metadata).await?;
                Ok(graph.id)
            }
            Err(ZFError::Empty) => {
                // this is a new addition, simple case.

                self.store.add_graph(&graph).await?;
                Ok(graph.id)
            }
            Err(e) => Err(e), //this is any other error so we propagate it
        }
    }

    async fn add_operator(
        &self,
        operator: RegistryComponent,
        tag: Option<String>,
    ) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }

    async fn add_sink(&self, sink: RegistryComponent, tag: Option<String>) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }

    async fn add_source(
        &self,
        source: RegistryComponent,
        tag: Option<String>,
    ) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }
}
