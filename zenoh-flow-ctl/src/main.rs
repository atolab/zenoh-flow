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

#![allow(clippy::upper_case_acronyms)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate prettytable;
extern crate base64;
extern crate exitfailure;

use clap::arg_enum;
use exitfailure::ExitFailure;
use git_version::git_version;
use structopt::StructOpt;
use uuid::Uuid;

const GIT_VERSION: &str = git_version!(prefix = "v", cargo_prefix = "v");

#[derive(StructOpt, Debug)]
pub enum AddKind {
    Flow {
        #[structopt(parse(from_os_str), name = "Flow descriptor path")]
        descriptor_path: std::path::PathBuf,
    },
    Instance {
        flow_id: String,
        #[structopt(short = "r", long = "rt-id", name = "Runtime UUID")]
        rt_id: Option<Uuid>,
    },
}

#[derive(StructOpt, Debug)]
pub enum GetKind {
    Flow { id: Option<String> },
    Instance { id: Option<Uuid> },
    Runtime { id: Option<Uuid> },
}

#[derive(StructOpt, Debug)]
pub enum DeleteKind {
    Flow { id: String },
    Instance { id: Uuid },
}


#[derive(StructOpt, Debug)]
pub enum ZFCtl {
    Add(AddKind),
    Get(GetKind),
    Delete(DeleteKind),
}

#[async_std::main]
async fn main() {
    env_logger::init();
    log::debug!("Eclipse Zenoh-Flow Ctl {}", GIT_VERSION);

    let args = ZFCtl::from_args();
    log::debug!("Args: {:?}", args);

    match args {
        ZFCtl::Add(ak) => {
            match ak {
                AddKind::Flow{descriptor_path} => {
                    println!("This is going to store the flow described in {:?}",descriptor_path);
                },
                AddKind::Instance{flow_id, rt_id} => {
                    println!("This is going to instantiate the flow {} in runtime {:?}", flow_id, rt_id);
                }

            }
        },
        ZFCtl::Get(gk) => {
            match gk {
                GetKind::Flow{id} => {
                    println!("This is going to get information for the flow {:?}", id);
                },
                GetKind::Instance{id} => {
                    println!("This is going to get information for the instance {:?}", id);
                },
                GetKind::Runtime{id} => {
                    println!("This is going to get information for the runtime {:?}", id);
                },
            }
        },
        ZFCtl::Delete(dk) => {
            match dk {
                DeleteKind::Flow{id} => {
                    println!("This is going to delete the flow {:?}",id);
                },
                DeleteKind::Instance{id} => {
                    println!("This is going to delete the instance {:?}", id);
                }
            }
        }
    }
}
