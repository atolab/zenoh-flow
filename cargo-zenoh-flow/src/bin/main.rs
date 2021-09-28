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

use async_std::process::exit;
use colored::*;
use structopt::StructOpt;

use zenoh_flow::model::component::{OperatorDescriptor, SinkDescriptor, SourceDescriptor};
use zenoh_flow::model::{
    ComponentKind, RegistryComponent, RegistryComponentArchitecture, RegistryComponentTag,
};
use zenoh_flow::OperatorId;

#[cfg(feature = "local_registry")]
use async_std::sync::Arc;
#[cfg(feature = "local_registry")]
use rand::seq::SliceRandom;
#[cfg(feature = "local_registry")]
use zenoh::*;
#[cfg(feature = "local_registry")]
use zenoh_flow::registry::RegistryClient;
#[cfg(feature = "local_registry")]
use zenoh_flow::registry::RegistryFileClient;

#[derive(StructOpt, Debug)]
pub enum ZFCtl {
    Build {
        #[structopt(short, long)]
        package: Option<String>,
        #[structopt(short = "m", long = "manifest-path", default_value = "Cargo.toml")]
        manifest_path: std::path::PathBuf,
        #[structopt(short, long)]
        release: bool,
        #[structopt(short = "t", long = "tag", default_value = "latest")]
        version_tag: String,
        cargo_build_flags: Vec<String>,
    },
    New {
        name: String,
        #[structopt(short = "k", long = "kind", default_value = "operator")]
        kind: ComponentKind,
    },
    List,
    Push {
        graph_id: String,
    },
    Pull {
        graph_id: String,
    },
}

#[async_std::main]
async fn main() {
    // `cargo zenoh-flow` invocation passes the `zenoh-flow` arg through.
    let mut args: Vec<String> = std::env::args().collect();
    args.remove(1);

    let args = ZFCtl::from_iter(args.iter());

    #[cfg(feature = "local_registry")]
    let znsession = match zenoh::net::open(
        Properties::from(String::from(
            "mode=peer;peer=unixsock-stream//tmp/zf-registry.sock",
        ))
        .into(),
    )
    .await
    {
        Ok(zn) => Arc::new(zn),
        Err(e) => {
            println!("{}: to create Zenoh session: {:?}", "error".red().bold(), e);
            exit(-1);
        }
    };

    #[cfg(feature = "local_registry")]
    let servers = match RegistryClient::find_servers(znsession.clone()).await {
        Ok(s) => s,
        Err(e) => {
            println!(
                "{}: to create find registry servers: {:?}",
                "error".red().bold(),
                e
            );
            exit(-1);
        }
    };

    #[cfg(feature = "local_registry")]
    let client = match servers.choose(&mut rand::thread_rng()) {
        Some(entry_point) => {
            log::debug!("Selected entrypoint runtime: {:?}", entry_point);
            let client = RegistryClient::new(znsession, *entry_point);
            Some(client)
        }
        None => {
            println!(
                "{}: unable to connect to local registry, component will not be uploaded.",
                "warning".yellow().bold()
            );
            None
        }
    };

    #[cfg(feature = "local_registry")]
    let zsession = match zenoh::Zenoh::new(Properties::from(String::from("mode=peer")).into()).await
    {
        Ok(z) => Arc::new(z),
        Err(e) => {
            println!("{}: to create Zenoh session: {:?}", "error".red().bold(), e);
            exit(-1);
        }
    };

    #[cfg(feature = "local_registry")]
    let file_client = RegistryFileClient::from(zsession);

    match args {
        ZFCtl::Build {
            package,
            manifest_path,
            release,
            version_tag,
            mut cargo_build_flags,
        } => {
            // `cargo zenoh-flow` invocation passes the `zenoh-flow` arg through.
            if cargo_build_flags
                .first()
                .map_or(false, |arg| arg == "zenoh-flow")
            {
                cargo_build_flags.remove(0);
            }

            let (component_info, target_dir, manifest_dir) =
                match cargo_zenoh_flow::utils::from_manifest(&manifest_path, package) {
                    Ok(res) => res,
                    Err(_e) => {
                        println!("{}: unable to parse Cargo.toml", "error".red().bold());
                        exit(-1);
                    }
                };
            let target = if release {
                format!(
                    "{}/release/{}{}{}",
                    target_dir.as_path().display().to_string(),
                    std::env::consts::DLL_PREFIX,
                    component_info.id,
                    std::env::consts::DLL_SUFFIX
                )
            } else {
                format!(
                    "{}/debug/{}{}{}",
                    target_dir.as_path().display().to_string(),
                    std::env::consts::DLL_PREFIX,
                    component_info.id,
                    std::env::consts::DLL_SUFFIX
                )
            };
            let uri = format!("file://{}", target);

            let (metadata_graph, _metadata_arch, descriptor) = match component_info.kind {
                ComponentKind::Operator => {
                    if component_info.inputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing inputs for Operator component",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    if component_info.outputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing outputs for Operator component",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    let inputs = component_info.inputs.unwrap();
                    let outputs = component_info.outputs.unwrap();

                    if inputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty inputs for Operator, it should have at least one input", "error".red().bold());
                        exit(-1);
                    }

                    if outputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty outputs for Operator, it should have at least one output", "error".red().bold());
                        exit(-1);
                    }

                    let descriptor = OperatorDescriptor {
                        id: OperatorId::from(component_info.id.clone()),
                        inputs: inputs.clone(),
                        outputs: outputs.clone(),
                        uri: Some(uri.clone()),
                        configuration: None,
                        runtime: None,
                    };

                    let metadata_arch = RegistryComponentArchitecture {
                        arch: String::from(std::env::consts::ARCH),
                        os: String::from(std::env::consts::OS),
                        uri,
                        checksum: String::from(""),
                        signature: String::from(""),
                    };

                    let metadata_tag = RegistryComponentTag {
                        #[cfg(feature = "local_registry")]
                        name: version_tag.clone(),
                        #[cfg(not(feature = "local_registry"))]
                        name: version_tag,
                        requirement_labels: vec![],
                        architectures: vec![metadata_arch.clone()],
                    };

                    let metadata_graph = RegistryComponent {
                        id: OperatorId::from(component_info.id.clone()),
                        kind: component_info.kind.clone(),
                        classes: vec![],
                        tags: vec![metadata_tag],
                        inputs,
                        outputs,
                        period: None,
                    };

                    let yml_descriptor = match serde_yaml::to_string(&descriptor) {
                        Ok(yml) => yml,
                        Err(e) => {
                            println!("{}: unable to serialize descriptor {}", "error".red(), e);
                            exit(-1)
                        }
                    };
                    (metadata_graph, metadata_arch, yml_descriptor)
                }
                ComponentKind::Source => {
                    if component_info.inputs.is_some() {
                        println!("{}: Zenoh-Flow metadata has inputs for Source component, they will be discarded", "warning".yellow().bold());
                    }

                    if component_info.outputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing outputs for Source component",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    let outputs = component_info.outputs.unwrap();

                    if outputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty outputs for Source, it should exactly one output", "error".red().bold());
                        exit(-1);
                    }

                    if outputs.len() > 1 {
                        println!("{}: Zenoh-Flow metadata has more than one output for Source, it should exactly one output", "error".red().bold());
                        exit(-1);
                    }

                    let output = &outputs[0];

                    let descriptor = SourceDescriptor {
                        id: OperatorId::from(component_info.id.clone()),
                        output: output.clone(),
                        uri: Some(uri.clone()),
                        configuration: None,
                        runtime: None,
                        period: None,
                    };

                    let metadata_arch = RegistryComponentArchitecture {
                        arch: String::from(std::env::consts::ARCH),
                        os: String::from(std::env::consts::OS),
                        uri,
                        checksum: String::from(""),
                        signature: String::from(""),
                    };

                    let metadata_tag = RegistryComponentTag {
                        #[cfg(feature = "local_registry")]
                        name: version_tag.clone(),
                        #[cfg(not(feature = "local_registry"))]
                        name: version_tag,
                        requirement_labels: vec![],
                        architectures: vec![metadata_arch.clone()],
                    };

                    let metadata_graph = RegistryComponent {
                        id: OperatorId::from(component_info.id.clone()),
                        kind: component_info.kind.clone(),
                        classes: vec![],
                        tags: vec![metadata_tag],
                        inputs: vec![],
                        outputs: vec![output.clone()],
                        period: None,
                    };

                    let yml_descriptor = match serde_yaml::to_string(&descriptor) {
                        Ok(yml) => yml,
                        Err(e) => {
                            println!("{}: unable to serialize descriptor {}", "error".red(), e);
                            exit(-1)
                        }
                    };
                    (metadata_graph, metadata_arch, yml_descriptor)
                }
                ComponentKind::Sink => {
                    if component_info.inputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing inputs for Sink component",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    if component_info.outputs.is_some() {
                        println!("{}: Zenoh-Flow metadata has outputs for Sink component, they will be discarded", "warning".yellow().bold());
                    }

                    let inputs = component_info.inputs.unwrap();

                    if inputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty inputs for Sink, it should exactly one inputs", "error".red().bold());
                        exit(-1);
                    }

                    if inputs.len() > 1 {
                        println!("{}: Zenoh-Flow metadata has more than one input for Sink, it should exactly one input", "error".red().bold());
                        exit(-1);
                    }

                    let input = &inputs[0];

                    let descriptor = SinkDescriptor {
                        id: OperatorId::from(component_info.id.clone()),
                        input: input.clone(),
                        uri: Some(uri.clone()),
                        configuration: None,
                        runtime: None,
                    };

                    let metadata_arch = RegistryComponentArchitecture {
                        arch: String::from(std::env::consts::ARCH),
                        os: String::from(std::env::consts::OS),
                        uri,
                        checksum: String::from(""),
                        signature: String::from(""),
                    };

                    let metadata_tag = RegistryComponentTag {
                        #[cfg(feature = "local_registry")]
                        name: version_tag.clone(),
                        #[cfg(not(feature = "local_registry"))]
                        name: version_tag,
                        requirement_labels: vec![],
                        architectures: vec![metadata_arch.clone()],
                    };

                    let metadata_graph = RegistryComponent {
                        id: OperatorId::from(component_info.id.clone()),
                        kind: component_info.kind.clone(),
                        classes: vec![],
                        tags: vec![metadata_tag],
                        inputs: vec![input.clone()],
                        outputs: vec![],
                        period: None,
                    };
                    let yml_descriptor = match serde_yaml::to_string(&descriptor) {
                        Ok(yml) => yml,
                        Err(e) => {
                            println!("{}: unable to serialize descriptor {}", "error".red(), e);
                            exit(-1)
                        }
                    };
                    (metadata_graph, metadata_arch, yml_descriptor)
                }
            };
            println!(
                "{} Component {} - Kind {}",
                "Compiling".green().bold(),
                component_info.id,
                component_info.kind.to_string()
            );

            match cargo_zenoh_flow::utils::cargo_build(&cargo_build_flags, release, &manifest_dir) {
                Ok(_) => (),
                Err(_) => {
                    println!("{}: cargo build failed", "error".red().bold());
                    exit(-1);
                }
            }
            match cargo_zenoh_flow::utils::store_zf_metadata(&metadata_graph, &target_dir) {
                Ok(res) => {
                    println!("{} stored in {}", "Metadata".green().bold(), res.bold());
                }
                Err(e) => {
                    println!("{}: failed to store metadata {:?}", "error".red().bold(), e);
                    exit(-1);
                }
            };

            match cargo_zenoh_flow::utils::store_zf_descriptor(
                &descriptor,
                &target_dir,
                &component_info.id,
            ) {
                Ok(res) => {
                    println!("{} stored in {}", "Descriptor".green().bold(), res.bold());
                }
                Err(e) => {
                    println!(
                        "{}: failed to store descriptor {:?}",
                        "error".red().bold(),
                        e
                    );
                    exit(-1);
                }
            }

            #[cfg(feature = "local_registry")]
            if client.is_some() {
                println!(
                    "{} {} to local registry",
                    "Uploading".green().bold(),
                    component_info.id.bold()
                );
                client
                    .as_ref()
                    .unwrap()
                    .add_graph(metadata_graph)
                    .await
                    .unwrap()
                    .unwrap();
            }

            #[cfg(feature = "local_registry")]
            if client.is_some() {
                println!(
                    "{} {} to local registry",
                    "Uploading".green().bold(),
                    target.bold()
                );
                file_client
                    .send_component(
                        &std::path::PathBuf::from(target),
                        &component_info.id,
                        &_metadata_arch,
                        &version_tag,
                    )
                    .await
                    .unwrap();
            }
            let build_target = if release {
                String::from("release")
            } else {
                String::from("debug")
            };
            println!(
                "{} [{}] component {} ",
                "Finished".green().bold(),
                build_target,
                component_info.id
            );
        }
        ZFCtl::New { name, kind } => {
            match cargo_zenoh_flow::utils::create_crate(&name, kind.clone()).await {
                Ok(_) => {
                    println!(
                        "{} boilerplate for {} {} ",
                        "Created".green().bold(),
                        kind.to_string(),
                        name.bold()
                    );
                }
                Err(_) => {
                    println!(
                        "{}: failed to create boilerplate for {} {}",
                        "error".red().bold(),
                        kind.to_string(),
                        name.bold(),
                    );
                }
            }
        }
        ZFCtl::List => {
            #[cfg(feature = "local_registry")]
            match client {
                Some(client) => {
                    println!("{:?}", client.get_all_graphs().await);
                }
                None => println!("Offline mode!"),
            }
            #[cfg(not(feature = "local_registry"))]
            println!("Offline mode!")
        }
        _ => unimplemented!("Not yet..."),
    }
}
