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

#![recursion_limit = "256"]

use async_std::fs;
use async_std::path::Path;
use async_std::sync::Arc;
use clap::{Arg, ArgMatches};
use flume::{bounded, Receiver, Sender};
use std::convert::TryFrom;
use zenoh::net::runtime::Runtime;
use zenoh::net::Session;
use zenoh_flow_registry::registry::{RegistryConfig, ZFRegistry};
use zenoh_plugin_trait::prelude::*;
use zenoh_util::core::{ZError, ZErrorKind};
use zenoh_util::zerror2;

pub struct ExamplePlugin {}

zenoh_plugin_trait::declare_plugin!(ExamplePlugin);

pub struct RegistryPluginStopper {
    sender: Arc<Sender<()>>,
}

impl PluginStopper for RegistryPluginStopper {
    fn stop(&self) {
        self.sender.send(()).unwrap()
    }
}

impl Plugin for ExamplePlugin {
    type Requirements = Vec<Arg<'static, 'static>>;
    type StartArgs = (Runtime, ArgMatches<'static>);
    fn compatibility() -> zenoh_plugin_trait::PluginId {
        zenoh_plugin_trait::PluginId {
            uid: "zenoh-example-plugin",
        }
    }

    fn get_requirements() -> Self::Requirements {
        vec![
            Arg::from_usage("--registry-config 'The selection of resources to be stored'")
                .default_value("/etc/zenoh/zf-registy.yaml"),
        ]
    }

    fn start(
        (runtime, args): &Self::StartArgs,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, Box<dyn std::error::Error>> {
        if let Some(config) = args.value_of("registry-config") {
            let (s, r) = bounded::<()>(1);
            let stopper = RegistryPluginStopper {
                sender: Arc::new(s),
            };

            async_std::task::spawn(run(runtime.clone(), config.into(), r));
            Ok(Box::new(stopper))
        } else {
            Err(Box::new(zerror2!(ZErrorKind::Other {
                descr: "config is a mandatory option for Zenoh Flow registry plugin".into()
            })))
        }
    }
}

async fn read_file(path: &Path) -> String {
    fs::read_to_string(path).await.unwrap()
}

async fn run(runtime: Runtime, config: String, stopper: Receiver<()>) {
    env_logger::init();

    // This is not used because we need both a Session and a Zenoh for the registry.
    // This will change once zenoh and zenoh::net apli are merged
    let _session = Session::init(runtime, true, vec![], vec![]).await;

    let conf_file_path = Path::new(&config);

    let conf = RegistryConfig::try_from(read_file(conf_file_path).await).unwrap();

    log::debug!(
        "Run Zenoh Flow registry plugin with config path={:?}",
        conf_file_path
    );

    // The registry will create its own zenoh::net and zenoh sessions.
    let registry = ZFRegistry::try_from(conf).unwrap();

    let (s, h) = registry.start().await.unwrap();

    // Waiting to be stopped by zenohd
    let _ = stopper.recv_async().await.unwrap();

    registry.stop(s).await.unwrap();
    h.await.unwrap();
}
